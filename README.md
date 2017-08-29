# sliding-window-storm
storm 1.1 的实现滑动窗口

storm1.0支持的时间和数量的排列组合有如下：
```
//Sliding window 窗口长度：tuple数, 滑动间隔: tuple数
withWindow(Count windowLength, Count slidingInterval)

//Sliding window 窗口长度：tuple数, 滑动间隔: 每个tuple滑一滑
withWindow(Count windowLength)

//Sliding window 窗口长度：tuple数, 滑动间隔: 时间
withWindow(Count windowLength, Duration slidingInterval)

//Sliding window 窗口长度：tuple数, 滑动间隔: 时间
withWindow(Duration windowLength, Duration slidingInterval)

//Sliding window 窗口长度：时间, 滑动间隔: 每个tuple滑一滑
withWindow(Duration windowLength)

//Sliding window 窗口长度：时间, 滑动间隔: 时间
withWindow(Duration windowLength, Count slidingInterval)

//Tumblingwindow 窗口长度：Tuple数
withTumblingWindow(BaseWindowedBolt.Count count)

//Tumblingwindow 窗口长度：时间
withTumblingWindow(BaseWindowedBolt.Duration duration)
```
