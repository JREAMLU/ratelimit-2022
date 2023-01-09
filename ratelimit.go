// Copyright 2014 Canonical Ltd.
// Licensed under the LGPLv3 with static-linking exception.
// See LICENCE file for details.

// Package ratelimit provides an efficient token bucket implementation
// that can be used to limit the rate of arbitrary things.
// See http://en.wikipedia.org/wiki/Token_bucket.
package ratelimit

import (
	"math"
	"strconv"
	"sync"
	"time"
)

// The algorithm that this implementation uses does computational work
// only when tokens are removed from the bucket, and that work completes
// in short, bounded-constant time (Bucket.Wait benchmarks at 175ns on
// my laptop).
//
// Time is measured in equal measured ticks, a given interval
// (fillInterval) apart. On each tick a number of tokens (quantum) are
// added to the bucket.
//
// When any of the methods are called the bucket updates the number of
// tokens that are in the bucket, and it records the current tick
// number too. Note that it doesn't record the current time - by
// keeping things in units of whole ticks, it's easy to dish out tokens
// at exactly the right intervals as measured from the start time.
//
// This allows us to calculate the number of tokens that will be
// available at some time in the future with a few simple arithmetic
// operations.
//
// The main reason for being able to transfer multiple tokens on each tick
// is so that we can represent rates greater than 1e9 (the resolution of the Go
// time package) tokens per second, but it's also useful because
// it means we can easily represent situations like "a person gets
// five tokens an hour, replenished on the hour".

// Bucket represents a token bucket that fills at a predetermined rate.
// Methods on Bucket may be called concurrently.
//
// 桶
//
// 每个fillInterval(填充周期)间隔的tick(刻度/周期/标记位) 会有令牌添加到桶里
//
// 当调用任何方法时, 存储桶会更新存储桶中的令牌数, 并且它也会记录当前的刻度数.
// 请注意, 它不记录当前时间 - 通过以整个刻度为单位保持事物, 从开始时间测量, 很容易以恰当的间隔标出令牌。
//
// 这允许我们通过一些简单的算术运算来计算将来某个时间可用的令牌数
//
// 能够在每个刻度上传输多个令牌的主要原因是我们可以表示大于1e9(Go时间包的分辨率) 每秒令牌的速率, 但它也很有用, 因为它意味着我们可以很容易地表示像"一个人每小时得到五个代币, 按小时补充"
//
// fillInterval (填充周期) 保存每个tick之间的间隔
//
// availableTokens 可用令牌数
// 关联的latestTick中的令牌
// 当有消费者时, 它将拒绝
// 等待令牌
type Bucket struct {
	clock Clock

	// startTime holds the moment when the bucket was
	// first created and ticks began.
	//
	// startTime保存了第一次创建桶并开始计时的时间.
	startTime time.Time

	// capacity holds the overall capacity of the bucket.
	//
	// capacity 桶的总容量
	capacity int64

	// quantum holds how many tokens are added on
	// each tick.
	//
	// quantum 每个tick上添加多少个令牌
	// tick 根据fillInterval来计算, 当quantum=1时 tick基本就是令牌数量
	// availableTokens += (tick - latestTick) * quantum
	quantum int64

	// fillInterval holds the interval between each tick.
	//
	// fillInterval 每个tick之间的间隔
	fillInterval time.Duration

	// mu guards the fields below it.
	mu sync.Mutex

	// availableTokens holds the number of available
	// tokens as of the associated latestTick.
	// It will be negative when there are consumers
	// waiting for tokens.
	//
	// availableTokens保存相关latestTick中可用令牌的数量.
	// 当有消费者等待令牌时, 它将拒绝.
	availableTokens int64

	// latestTick holds the latest tick for which
	// we know the number of tokens in the bucket.
	//
	// latestTick保存最新的刻度, 我们知道桶中的令牌数量
	latestTick int64
}

// NewBucket returns a new token bucket that fills at the
// rate of one token every fillInterval, up to the given
// maximum capacity. Both arguments must be
// positive. The bucket is initially full.
//
// NewBucket返回一个新的令牌桶, 每个fillInterval以一个令牌的速率填充, 直到达到给定的最大容量. 两个论点都必须是积极的. 水桶最初已满
func NewBucket(fillInterval time.Duration, capacity int64) *Bucket {
	return NewBucketWithClock(fillInterval, capacity, nil)
}

// NewBucketWithClock is identical to NewBucket but injects a testable clock
// interface.
//
// NewBucketWithClock与NewBucket相同, 但注入了可测试的时钟接口
func NewBucketWithClock(fillInterval time.Duration, capacity int64, clock Clock) *Bucket {
	return NewBucketWithQuantumAndClock(fillInterval, capacity, 1, clock)
}

// rateMargin specifes the allowed variance of actual
// rate from specified rate. 1% seems reasonable.
//
// rate Margin指定允许的实际速率与指定速率的差异, 1％似乎合理
const rateMargin = 0.01

// NewBucketWithRate returns a token bucket that fills the bucket
// at the rate of rate tokens per second up to the given
// maximum capacity. Because of limited clock resolution,
// at high rates, the actual rate may be up to 1% different from the
// specified rate.
//
// NewBucketWithRate返回一个令牌桶, 它以每秒速率(令牌的速率)填充桶, 直到给定的最大容量.
// 由于时钟分辨率有限, 在高速率下, 实际速率可能与指定速率不同, 最高可达1％不同
// rate 2000 每秒2000个令牌
// capacity 10000 总共10000个令牌
func NewBucketWithRate(rate float64, capacity int64) *Bucket {
	return NewBucketWithRateAndClock(rate, capacity, nil)
}

// NewBucketWithRateAndClock is identical to NewBucketWithRate but injects a
// testable clock interface.
//
// NewBucketWithRateAndClock与NewBucketWithRate相同, 但注入了可测试的时钟接口
func NewBucketWithRateAndClock(rate float64, capacity int64, clock Clock) *Bucket {
	// Use the same bucket each time through the loop
	// to save allocations.
	tb := NewBucketWithQuantumAndClock(1, capacity, 1, clock)
	for quantum := int64(1); quantum < 1<<50; quantum = nextQuantum(quantum) {
		fillInterval := time.Duration(1e9 * float64(quantum) / rate)
		if fillInterval <= 0 {
			continue
		}
		tb.fillInterval = fillInterval
		tb.quantum = quantum
		if diff := math.Abs(tb.Rate() - rate); diff/rate <= rateMargin {
			return tb
		}
	}
	panic("cannot find suitable quantum for " + strconv.FormatFloat(rate, 'g', -1, 64))
}

// nextQuantum returns the next quantum to try after q.
// We grow the quantum exponentially, but slowly, so we
// get a good fit in the lower numbers.
//
// nextQuantum返回下一个量子以在q之后尝试。 我们以指数方式增长量子, 但速度很慢, 所以我们在较低的数字中得到了很好的拟合
// 类似+n n线性增长
func nextQuantum(q int64) int64 {
	q1 := q * 11 / 10
	if q1 == q {
		q1++
	}
	return q1
}

// NewBucketWithQuantum is similar to NewBucket, but allows
// the specification of the quantum size - quantum tokens
// are added every fillInterval.
//
// NewBucketWithQuantum类似于NewBucket, 但允许指定quantum大小 - 每个fillInterval都会添加quantum令牌
func NewBucketWithQuantum(fillInterval time.Duration, capacity, quantum int64) *Bucket {
	return NewBucketWithQuantumAndClock(fillInterval, capacity, quantum, nil)
}

// NewBucketWithQuantumAndClock is like NewBucketWithQuantum, but
// also has a clock argument that allows clients to fake the passing
// of time. If clock is nil, the system clock will be used.
//
// NewBucketWithQuantumAndClock就像NewBucketWithQuantum, 但也有一个时钟参数, 允许客户伪造时间的流逝。 如果时钟为零, 则使用系统时钟.
func NewBucketWithQuantumAndClock(fillInterval time.Duration, capacity, quantum int64, clock Clock) *Bucket {
	if clock == nil {
		clock = realClock{}
	}
	if fillInterval <= 0 {
		panic("token bucket fill interval is not > 0")
	}
	if capacity <= 0 {
		panic("token bucket capacity is not > 0")
	}
	if quantum <= 0 {
		panic("token bucket quantum is not > 0")
	}
	return &Bucket{
		clock:           clock,
		startTime:       clock.Now(),
		latestTick:      0,
		fillInterval:    fillInterval,
		capacity:        capacity,
		quantum:         quantum,
		availableTokens: capacity,
	}
}

// Wait takes count tokens from the bucket, waiting until they are
// available.
//
// 等待可以用的令牌
func (tb *Bucket) Wait(count int64) {
	if d := tb.Take(count); d > 0 {
		tb.clock.Sleep(d)
	}
}

// WaitMaxDuration is like Wait except that it will
// only take tokens from the bucket if it needs to wait
// for no greater than maxWait. It reports whether
// any tokens have been removed from the bucket
// If no tokens have been removed, it returns immediately.
//
// WaitMaxDuration类似于Wait, 只有等待不超过maxWait时才能从桶中取出令牌. 它会报告是否已从桶中删除令牌 如果没有删除令牌, 则立即返回.
func (tb *Bucket) WaitMaxDuration(count int64, maxWait time.Duration) bool {
	d, ok := tb.TakeMaxDuration(count, maxWait)
	if d > 0 {
		tb.clock.Sleep(d)
	}
	return ok
}

// 无穷大的时间 2562047h47m16.854775807s  292年+
const infinityDuration time.Duration = 0x7fffffffffffffff

// Take takes count tokens from the bucket without blocking. It returns
// the time that the caller should wait until the tokens are actually
// available.
//
// Note that if the request is irrevocable - there is no way to return
// tokens to the bucket once this method commits us to taking them.
//
// 从桶中取出计数令牌而不会阻塞. 它返回调用者应该等到令牌实际可用的时间.
// 请注意, 如果请求是不可撤销的 - 一旦调用了此方法, 就无法将令牌返回到存储桶.
// maxWait = infinityDuration
func (tb *Bucket) Take(count int64) time.Duration {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	// 因为maxWait = infinityDuration, 触发不到返回false的逻辑, 所以返回的第二个参数没必要
	d, _ := tb.take(tb.clock.Now(), count, infinityDuration)
	return d
}

// TakeMaxDuration is like Take, except that
// it will only take tokens from the bucket if the wait
// time for the tokens is no greater than maxWait.
//
// If it would take longer than maxWait for the tokens
// to become available, it does nothing and reports false,
// otherwise it returns the time that the caller should
// wait until the tokens are actually available, and reports
// true.
//
// TakeMaxDuration就像Take, 只是如果令牌的等待时间不超过maxWait, 它只会从桶中获取令牌.
// 如果令牌变得可用的时间大于maxWait, 返回false, 否则它返回 调用者应该等到令牌实际可用的时间(还有多久才能使用令牌的时间), 并返回true
func (tb *Bucket) TakeMaxDuration(count int64, maxWait time.Duration) (time.Duration, bool) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	return tb.take(tb.clock.Now(), count, maxWait)
}

// TakeAvailable takes up to count immediately available tokens from the
// bucket. It returns the number of tokens removed, or zero if there are
// no available tokens. It does not block.
//
// TakeAvailable需要从桶中计算立即可用的令牌. 它返回已删除的令牌数, 如果没有可用令牌, 则返回零. 无阻塞
// call前调用这个方法 返回0时已限流
// count 需要花费的令牌数 一般为1
func (tb *Bucket) TakeAvailable(count int64) int64 {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	return tb.takeAvailable(tb.clock.Now(), count)
}

// takeAvailable is the internal version of TakeAvailable - it takes the
// current time as an argument to enable easy testing.
//
// takeAvailable是TakeAvailable的内部版本 - 它将当前时间作为参数来启用简单测试.
func (tb *Bucket) takeAvailable(now time.Time, count int64) int64 {
	if count <= 0 {
		return 0
	}

	// 增加可用令牌数, 例如在1s内 请求过多, 令牌增加有限
	tb.adjustavailableTokens(tb.currentTick(now))
	if tb.availableTokens <= 0 {
		return 0
	}

	// 当可用的为0时, count=0
	if count > tb.availableTokens {
		count = tb.availableTokens
	}

	// 减少可用令牌数
	// 可用的减去花费的
	tb.availableTokens -= count

	// 返回已删除的令牌, 0时表明无令牌可用
	return count
}

// Available returns the number of available tokens. It will be negative
// when there are consumers waiting for tokens. Note that if this
// returns greater than zero, it does not guarantee that calls that take
// tokens from the buffer will succeed, as the number of available
// tokens could have changed in the meantime. This method is intended
// primarily for metrics reporting and debugging.
//
// 返回可用令牌的数量. 当有消费者等待令牌时, 它将拒绝.
// 请注意, 如果返回大于零, 则不能保证从缓冲区获取令牌的调用将成功, 因为可用令牌的数量在此期间可能已更改. 此方法原来主要用于merics报告和调试.
func (tb *Bucket) Available() int64 {
	return tb.available(tb.clock.Now())
}

// available is the internal version of available - it takes the current time as
// an argument to enable easy testing.
func (tb *Bucket) available(now time.Time) int64 {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.adjustavailableTokens(tb.currentTick(now))
	return tb.availableTokens
}

// Capacity returns the capacity that the bucket was created with.
// Capacity 返回创建桶的容量.
func (tb *Bucket) Capacity() int64 {
	return tb.capacity
}

// Rate returns the fill rate of the bucket, in tokens per second.
// Rate返回桶的填充率, 以每秒标记为单位.
func (tb *Bucket) Rate() float64 {
	return 1e9 * float64(tb.quantum) / float64(tb.fillInterval)
}

// take is the internal version of Take - it takes the current time as
// an argument to enable easy testing.
func (tb *Bucket) take(now time.Time, count int64, maxWait time.Duration) (time.Duration, bool) {
	if count <= 0 {
		return 0, true
	}

	tick := tb.currentTick(now)
	tb.adjustavailableTokens(tick)
	avail := tb.availableTokens - count
	// 有可用令牌 返回0 一般sleep(0)
	if avail >= 0 {
		tb.availableTokens = avail
		return 0, true
	}
	// Round up the missing tokens to the nearest multiple
	// of quantum - the tokens won't be available until
	// that tick.

	// endTick holds the tick when all the requested tokens will
	// become available.
	endTick := tick + (-avail+tb.quantum-1)/tb.quantum
	endTime := tb.startTime.Add(time.Duration(endTick) * tb.fillInterval)
	waitTime := endTime.Sub(now)
	// Take时 maxWait为无穷大
	if waitTime > maxWait {
		return 0, false
	}
	tb.availableTokens = avail

	// 返回等待时间 sleep(waitTime)
	return waitTime, true
}

// currentTick returns the current time tick, measured
// from tb.startTime.
//
// currentTick返回当前时间刻度, 从tb.startTime开始计算
func (tb *Bucket) currentTick(now time.Time) int64 {
	return int64(now.Sub(tb.startTime) / tb.fillInterval)
}

// adjustavailableTokens adjusts the current number of tokens
// available in the bucket at the given time, which must
// be in the future (positive) with respect to tb.latestTick.
//
// 调整在给定时间桶中当前可用的令牌数量, tb.latestTick 正的
func (tb *Bucket) adjustavailableTokens(tick int64) {
	lastTick := tb.latestTick
	tb.latestTick = tick
	if tb.availableTokens >= tb.capacity {
		return
	}
	tb.availableTokens += (tick - lastTick) * tb.quantum
	if tb.availableTokens > tb.capacity {
		tb.availableTokens = tb.capacity
	}
}

// Clock represents the passage of time in a way that
// can be faked out for tests.
//
// 时钟 用来做测试
type Clock interface {
	// Now returns the current time.
	Now() time.Time
	// Sleep sleeps for at least the given duration.
	Sleep(d time.Duration)
}

// realClock implements Clock in terms of standard time functions.
type realClock struct{}

// Now implements Clock.Now by calling time.Now.
func (realClock) Now() time.Time {
	return time.Now()
}

// Now implements Clock.Sleep by calling time.Sleep.
func (realClock) Sleep(d time.Duration) {
	time.Sleep(d)
}
