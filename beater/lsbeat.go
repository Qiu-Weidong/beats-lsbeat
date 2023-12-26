package beater

/*
 * 扫描整个数据盘，首先找到所有的 list 目录 LOG 目录和job目录
 * 扫描间隔设置很长, 数个小时扫描一次

 registrar-list.json
 [{ "path": "/xxx/xxx/list": { "filename": "xxx.list", "collect_time": xxx } }]

 registrar-log.json
 [{ "path": "/xxx/xxx/LOG": { "filename": "xxx.log", "collect_time": xxx } }]
*/

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"

	"github.com/Qiu-Weidong/lsbeat/config"
)

// lsbeat configuration.
type lsbeat struct {
	done   chan struct{}
	config config.Config
	client beat.Client

	lastIndexTime time.Time

	registrar_list map[string]map[string]time.Time
	registrar_log  map[string]map[string]time.Time
}

// New creates an instance of lsbeat.
func New(b *beat.Beat, cfg *common.Config) (beat.Beater, error) {

	c := config.DefaultConfig
	if err := cfg.Unpack(&c); err != nil {
		return nil, fmt.Errorf("Error reading config file: %v", err)
	}

	if !strings.HasSuffix(c.RegistrarListPath, ".json") {
		// 需要添加文件名
		c.RegistrarListPath = filepath.Join(c.RegistrarListPath, "registrar-list.json")
	}

	if !strings.HasSuffix(c.RegistrarLogPath, ".json") {
		// 需要添加文件名
		c.RegistrarLogPath = filepath.Join(c.RegistrarLogPath, "registrar-log.json")
	}

	bt := &lsbeat{
		done:   make(chan struct{}),
		config: c,

		// 初始化 lastIndexTime
		lastIndexTime: time.Now(),

		registrar_list: loadRegistrar(c.RegistrarListPath),
		registrar_log:  loadRegistrar(c.RegistrarLogPath),
	}
	return bt, nil
}

// Run starts lsbeat.
func (bt *lsbeat) Run(b *beat.Beat) error {
	logp.Info("lsbeat is running! Hit CTRL-C to stop it.")

	var err error
	bt.client, err = b.Publisher.Connect()
	if err != nil {
		return err
	}

	ticker := time.NewTicker(bt.config.Period)

	cnt := bt.config.Cycles
	list_path := []string{}
	log_path := []string{}
	for {
		select {
		case <-bt.done:
			return nil
		case <-ticker.C:
		}

		cnt += 1

		if cnt >= bt.config.Cycles {
			cnt = 0
			// 搜索一遍所有的 list 目录和 LOG 目录
			list_path = findDirectories(bt.config.Path, "list")
			log_path = findDirectories(bt.config.Path, "LOG")
		}

		// 移除掉已经不存在的条目
		existingDirectories := []string{}
		for _, dir := range list_path {
			if _, err := os.Stat(dir); err == nil {
				// 目录存在，将其添加到新的目录列表中
				existingDirectories = append(existingDirectories, dir)
			}
		}
		list_path = existingDirectories

		existingDirectories = []string{}
		for _, dir := range log_path {
			if _, err := os.Stat(dir); err == nil {
				// 目录存在，将其添加到新的目录列表中
				existingDirectories = append(existingDirectories, dir)
			}
		}
		log_path = existingDirectories

		// 采集 list 文件
		for _, p := range list_path {
			// 记得测试一下目录是否存在
			bt.collectList(p, b)
		}
		// 采集 log 文件
		for _, p := range log_path {
			bt.collectLog(p, b)
		}

		logp.Info("Event sent")
	}
}

// Stop stops lsbeat.
func (bt *lsbeat) Stop() {
	bt.client.Close()
	close(bt.done)
}

// 查找所有的 list 目录
func findDirectories(roots []string, target string) []string {
	var directories []string

	for _, root := range roots {
		err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {

			if err != nil {
				return err
			}

			if info.IsDir() && info.Name() == target {
				directories = append(directories, path)
			}
			return nil
		})
		if err != nil {
			continue
		}
	}

	return directories
}

func (bt *lsbeat) collectList(listDir string, b *beat.Beat) {
	now := time.Now()
	modified := false

	// 采集 list 目录下的所有 list 文件
	files, err := os.ReadDir(listDir)
	if err != nil {
		logp.Err("can not open dir %s", listDir)
	} else {
		for _, file := range files {
			if !file.IsDir() && filepath.Ext(file.Name()) == ".list" {
				info, err := file.Info()
				if err != nil {
					logp.Err("can not info file %s", file.Name())
				}
				modTime := info.ModTime()
				last := bt.list_collect_time(listDir, file.Name())

				if last == nil || last.Before(modTime) {
					bt.sendList(listDir, file.Name(), b, modTime)
					modified = true
				}
			}
		}
	}
	if modified {
		saveRegistrar(bt.config.RegistrarListPath, bt.registrar_list)
	}
	bt.lastIndexTime = now
}

func (bt *lsbeat) collectLog(logDir string, b *beat.Beat) {
	now := time.Now()
	modified := false

	// 采集 list 目录下的所有 list 文件
	files, err := os.ReadDir(logDir)
	if err != nil {
		logp.Err("can not open dir %s", logDir)
	} else {
		for _, file := range files {
			if !file.IsDir() && filepath.Ext(file.Name()) == ".log" {
				info, err := file.Info()
				if err != nil {
					logp.Err("can not info file %s", file.Name())
				}
				modTime := info.ModTime()
				last := bt.log_collect_time(logDir, file.Name())

				if last == nil || last.Before(modTime) {
					bt.sendLog(logDir, file.Name(), b, modTime)
					modified = true
				}
			}
		}
	}
	if modified {
		saveRegistrar(bt.config.RegistrarLogPath, bt.registrar_log)
	}
	bt.lastIndexTime = now
}

func (bt *lsbeat) list_collect_time(path string, filename string) *time.Time {
	// 获取 list 文件的采集时间
	value, ok := bt.registrar_list[path]
	if ok {
		v, o := value[filename]
		if o {
			return &v
		}
	}
	return nil
}
func (bt *lsbeat) log_collect_time(path string, filename string) *time.Time {
	// 获取 list 文件的采集时间
	value, ok := bt.registrar_log[path]
	if ok {
		v, o := value[filename]
		if o {
			return &v
		}
	}
	return nil
}

// 这后边的代码应该没什么问题
// 发送 list 文件
func (bt *lsbeat) sendList(path string, filename string, b *beat.Beat, modtime time.Time) {
	now := time.Now()

	// 更新 registrar
	value, ok := bt.registrar_list[path]
	if ok {
		// 键存在, 将 filename 添加到 value 中
		value[filename] = now
	} else {
		// 键不存在
		bt.registrar_list[path] = map[string]time.Time{
			filename: now,
		}
	}

	fullPath := filepath.Join(path, filename)
	content, err := os.ReadFile(fullPath)
	if err != nil {
		logp.Err("can not read file %s", fullPath)
		return
	}

	// 直接将文件内容发送过去即可
	event := beat.Event{
		Timestamp: now,
		Fields: common.MapStr{
			"type":     "list",
			"filename": filename,
			"path":     fullPath,
			"modtime":  modtime,
			"content":  string(content),
		},
	}
	bt.client.Publish(event)
}

// 发送 log 文件
func (bt *lsbeat) sendLog(path string, filename string, b *beat.Beat, modtime time.Time) {
	now := time.Now()

	// 更新 registrar
	value, ok := bt.registrar_log[path]
	if ok {
		// 键存在, 将 filename 添加到 value 中
		value[filename] = now
	} else {
		// 键不存在
		bt.registrar_log[path] = map[string]time.Time{
			filename: now,
		}
	}

	fullPath := filepath.Join(path, filename)
	content, err := os.ReadFile(fullPath)
	if err != nil {
		logp.Err("can not read file %s", fullPath)
		return
	}

	// 直接将文件内容发送过去即可
	event := beat.Event{
		Timestamp: now,
		Fields: common.MapStr{
			"type":     "log",
			"filename": filename,
			"path":     fullPath,
			"modtime":  modtime,
			"content":  string(content),
		},
	}
	bt.client.Publish(event)
}

type item struct {
	Path  string      `json:"path"`
	Files []childItem `json:"files"`
}

type childItem struct {
	Filename      string    `json:"filename"`
	CollectedTime time.Time `json:"collected_time"`
}

func loadRegistrar(registrarPath string) map[string]map[string]time.Time {
	// 加载文件采集的数据

	m := map[string]map[string]time.Time{}
	content, err := os.ReadFile(registrarPath)
	if err != nil {
		return m
	}
	var items []item
	err = json.Unmarshal(content, &items)
	if err != nil {
		return m
	}

	for _, item := range items {
		childitem := map[string]time.Time{}
		for _, child := range item.Files {
			childitem[child.Filename] = child.CollectedTime
		}
		m[item.Path] = childitem
	}

	return m
}

func saveRegistrar(registrarPath string, m map[string]map[string]time.Time) {
	// 首先判断目录是否存在

	// 获取路径的目录部分
	dir := filepath.Dir(registrarPath)
	if _, err := os.Stat(dir); err != nil && os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			logp.Err("can not mkdir %s", dir)
			return
		}
	}

	file, err := os.Create(registrarPath)
	if err != nil {
		logp.Err("can not open file %s", registrarPath)
		return
	}
	defer file.Close()

	var items []item
	for key, value := range m {
		var childitems []childItem
		for key1, value1 := range value {
			childitems = append(childitems, childItem{Filename: key1, CollectedTime: value1})
		}
		items = append(items, item{Path: key, Files: childitems})
	}
	encoder := json.NewEncoder(file)
	err = encoder.Encode(items)

	if err != nil {
		logp.Err("fail to write registrar.")
	}
}

// func (bt *lsbeat) collect(baseDir string, b *beat.Beat) {
// 	now := time.Now()

// 	list_registrar_modified := false
// 	log_registrar_modified := false

// 	list_registrar_modified = bt.search_list(baseDir, b)
// 	log_registrar_modified = bt.search_log(baseDir, b)

// 	if list_registrar_modified {
// 		// 更新 list 对应的 registrar 文件
// 		saveRegistrar(bt.config.RegistrarListPath, bt.registrar_list)
// 	}
// 	if log_registrar_modified {
// 		// 更新 log 对应的 registrar 文件
// 		saveRegistrar(bt.config.RegistrarLogPath, bt.registrar_log)
// 	}
// 	if !list_registrar_modified && !log_registrar_modified {
// 		logp.Info("no file added at this period.")
// 	}

// 	// 更新时间
// 	bt.lastIndexTime = now
// }

// func (bt *lsbeat) search_list(currentDir string, b *beat.Beat) bool {
// 	result := false
// 	entries, err := os.ReadDir(currentDir)
// 	if err != nil {
// 		logp.Err("can't read dir %s", currentDir)
// 		return result
// 	}

// 	var list fs.DirEntry
// 	for _, entry := range entries {
// 		if entry.IsDir() && entry.Name() == "list" {
// 			// 如果有 list 目录, 那么就不需要再递归搜索了
// 			list = entry
// 			break
// 		}
// 	}

// 	if list != nil {
// 		// 查询 list 下面的 list 文件即可
// 		fullPath := filepath.Join(currentDir, list.Name())
// 		entries, err := os.ReadDir(fullPath)
// 		if err != nil {
// 			logp.Err("can't read dir %s", fullPath)
// 			return result
// 		}
// 		for _, entry := range entries {
// 			if filepath.Ext(entry.Name()) == ".list" {
// 				// 检查是否以 list 结尾
// 				info, err := entry.Info()
// 				if err != nil {
// 					logp.Err("can not info file %s", entry.Name())
// 				}
// 				modTime := info.ModTime()
// 				last := bt.list_collect_time(fullPath, entry.Name())
// 				if last == nil || last.Before(modTime) {
// 					// 没有采集过, 采集之
// 					bt.sendList(fullPath, entry.Name(), b, modTime)
// 					result = true
// 				}
// 			}
// 		}

// 	} else {
// 		// 递归搜索
// 		for _, entry := range entries {
// 			if entry.IsDir() {
// 				path := filepath.Join(currentDir, entry.Name())
// 				result = result || bt.search_list(path, b)
// 			} else if filepath.Ext(entry.Name()) == ".list" {
// 				// 检查是否以 list 结尾
// 				info, err := entry.Info()
// 				if err != nil {
// 					logp.Err("can not info file %s", entry.Name())
// 				}
// 				modTime := info.ModTime()
// 				last := bt.list_collect_time(currentDir, entry.Name())
// 				if last == nil || last.Before(modTime) {
// 					// 没有采集过, 采集之
// 					bt.sendList(currentDir, entry.Name(), b, modTime)
// 					result = true
// 				}

// 			}
// 		}
// 	}

// 	return result
// }

// func (bt *lsbeat) search_log(currentDir string, b *beat.Beat) bool {
// 	result := false
// 	entries, err := os.ReadDir(currentDir)
// 	if err != nil {
// 		logp.Err("can't read dir %s", currentDir)
// 		return result
// 	}

// 	var log fs.DirEntry
// 	for _, entry := range entries {
// 		if entry.IsDir() && entry.Name() == "LOG" {
// 			// 如果有 list 目录, 那么就不需要再递归搜索了
// 			log = entry
// 			break
// 		}
// 	}

// 	if log != nil {
// 		// 查询 list 下面的 list 文件即可
// 		fullPath := filepath.Join(currentDir, log.Name())
// 		entries, err := os.ReadDir(fullPath)
// 		if err != nil {
// 			logp.Err("can't read dir %s", fullPath)
// 			return result
// 		}
// 		for _, entry := range entries {
// 			if filepath.Ext(entry.Name()) == ".log" {
// 				// 检查是否以 list 结尾
// 				info, err := entry.Info()
// 				if err != nil {
// 					logp.Err("can not info file %s", entry.Name())
// 				}
// 				modTime := info.ModTime()
// 				last := bt.log_collect_time(fullPath, entry.Name())
// 				if last == nil || last.Before(modTime) {
// 					// 没有采集过, 采集之
// 					bt.sendLog(fullPath, entry.Name(), b, modTime)
// 					result = true
// 				}
// 			}
// 		}

// 	} else {
// 		// 递归搜索
// 		for _, entry := range entries {
// 			if entry.IsDir() {
// 				path := filepath.Join(currentDir, entry.Name())
// 				result = result || bt.search_log(path, b)
// 			} else if filepath.Ext(entry.Name()) == ".log" {
// 				// 检查是否以 list 结尾
// 				info, err := entry.Info()
// 				if err != nil {
// 					logp.Err("can not info file %s", entry.Name())
// 				}
// 				modTime := info.ModTime()
// 				last := bt.log_collect_time(currentDir, entry.Name())
// 				if last == nil || last.Before(modTime) {
// 					// 没有采集过, 采集之
// 					bt.sendLog(currentDir, entry.Name(), b, modTime)
// 					result = true
// 				}

// 			}
// 		}
// 	}

// 	return result
// }
