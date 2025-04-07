local key = KEYS[1]
local value = ARGV[1]
local expireOption = tonumber(ARGV[2]) --为0，则设置永不过期，如果为1，则过期时间为一个月，否则设置过期时间为今日剩余时间
-- 尝试添加元素到集合中并获取添加结果
local addResult = redis.call('SADD', key, value)
-- 如果addResult为0，表示元素已经存在于集合中
if addResult == 0 then
    return 0
end
-- 根据expireOption设置过期时间
if expireOption == 2 then
    -- 计算今日剩余秒数
    local currentTime = redis.call('TIME')
    local currentSeconds = tonumber(currentTime[1])
    local hours = tonumber(os.date("%H", currentSeconds))
    local minutes = tonumber(os.date("%M", currentSeconds))
    local seconds = tonumber(os.date("%S", currentSeconds))

    -- 计算从当前时间到今天结束的秒数
    local secondsUntilEndOfDay = (23 - hours) * 3600 + (59 - minutes) * 60 + (59 - seconds)
    -- 否则设置过期时间为今日剩余时间
    if secondsUntilEndOfDay > 0 then
        redis.call('EXPIRE', key, secondsUntilEndOfDay)
    end
elseif expireOption == 1 then
    -- 设置过期时间为一个月 (31天)
    local oneMonthInSeconds = 31 * 24 * 60 * 60
    redis.call('EXPIRE', key, oneMonthInSeconds)
else
    -- 设置永不过期
    redis.call('PERSIST', key)
end
return 1