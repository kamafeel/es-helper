#使用ES
绝大部分已经封装
ESIndexService  索引使用
ESTemplateService 文档使用

#使用REDIS
RedisService

#使用异步线程池
TaskPoolConfig里面注入
@Async("redisExecutor")

#使用缓存
RedisConfig里面注入

开启缓存
@Cacheable("shortCache")

清除缓存
@CacheEvict(value="shortCache",allEntries=true)
