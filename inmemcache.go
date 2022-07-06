package inmemcache

import (
	"sync"
	"time"

	"github.com/bluele/gcache"
)

type DATALOADER func(nFail int) (interface{}, time.Duration, error)

type MemCache struct {
	mpLoading map[string]int // 0:loading  1:need reloading
	failCount map[string]int //某个key加载失败的次数, 用于防止缓存击穿
	lock      sync.Mutex
	cache     gcache.Cache
}

func NewMemCache(size int) *MemCache {
	obj := &MemCache{
		mpLoading: make(map[string]int),
		failCount: make(map[string]int),
		cache:     gcache.New(size).LRU().Build(),
	}

	return obj
}

//szKey存在则立即返回，否则用fnLoader 函数加载值后返回
func (t *MemCache) Get(szKey string, fnLoader DATALOADER) (interface{}, error) {
	for {
		v, err := t.cache.Get(szKey)
		if err == nil {
			return v, nil
		}

		if err != gcache.KeyNotFoundError {
			return nil, err
		}

		if t._setKeyLoadingIfNotExist(szKey) {
			break
		}

		time.Sleep(time.Millisecond * 10)
	}

	defer t._delKeyLoading(szKey)

	var data interface{}
	var d time.Duration
	var err error

	nFail := t._getFailCount(szKey)

	data, d, err = fnLoader(nFail)
	if err != nil {
		t._addFailCount(szKey)
		return nil, err
	}

	if nFail > 0 {
		t._delFailCount(szKey)
	}

	if n := t._readKeyLoading(szKey); n != 1 {
		t.cache.SetWithExpire(szKey, data, d) //加载数据过程中数据没有另外被更新
	}

	return data, nil
}

func (t *MemCache) Has(szKey string) bool {
	return t.cache.Has(szKey)
}

func (t *MemCache) Delete(szKey string) {
	t._setKeyNeedReLoadIfInLoading(szKey)
	t.cache.Remove(szKey)
}

//获取加载数据的权限
func (t *MemCache) _setKeyLoadingIfNotExist(szKey string) bool {
	var bOK bool = false

	t.lock.Lock()
	if _, ok := t.mpLoading[szKey]; !ok {
		t.mpLoading[szKey] = 0
		bOK = true
	}
	t.lock.Unlock()

	return bOK
}

func (t *MemCache) _delKeyLoading(szKey string) {
	t.lock.Lock()
	delete(t.mpLoading, szKey)
	t.lock.Unlock()
}

//通知加载中的key需要重新加载
func (t *MemCache) _setKeyNeedReLoadIfInLoading(szKey string) {
	t.lock.Lock()
	if _, ok := t.mpLoading[szKey]; ok {
		t.mpLoading[szKey] = 1
	}
	t.lock.Unlock()
}

func (t *MemCache) _readKeyLoading(szKey string) int {
	var n int = -1
	t.lock.Lock()
	if _, ok := t.mpLoading[szKey]; ok {
		n = t.mpLoading[szKey]
	}
	t.lock.Unlock()
	return n
}

func (t *MemCache) _getFailCount(szKey string) int {
	t.lock.Lock()
	nCount, ok := t.failCount[szKey]
	t.lock.Unlock()
	if ok {
		return nCount
	} else {
		return 0
	}
}

func (t *MemCache) _addFailCount(szKey string) {
	t.lock.Lock()
	nCount, ok := t.failCount[szKey]
	if ok {
		t.failCount[szKey] = nCount + 1
	} else {
		t.failCount[szKey] = 1
	}
	t.lock.Unlock()
}

func (t *MemCache) _delFailCount(szKey string) {
	t.lock.Lock()
	delete(t.failCount, szKey)
	t.lock.Unlock()
}

/*
import (
	"fmt"
	"time"

	"github.com/carr123/inmemcache"
)

func main() {
	cache = inmemcache.NewMemCache(100)

	szCacheKey := "key1"
	value, err := cache.Get(szCacheKey, func(nFail int) (interface{}, time.Duration, error) {
		if nFail >= 3 { //防止缓存击穿, 10秒内的请求直接返回空数据
			return "", time.Second * 10, nil
		}

		if data, err := querydb(szCacheKey); err != nil {
			return nil, time.Second, err
		}else {
			return data, time.Second * 300, nil
		}
	})
	if err == nil {
		fmt.Println("value is:", value)
		return
	}

	time.Sleep(time.Hour * 100)
}


*/
