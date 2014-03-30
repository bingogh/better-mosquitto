better-mosquitto
================
Because there are several design in mosquitto are not good enough, so i decide to change it as to train my c.

## TODO
- event loop layer to support multi platform

## log
- 2014.3.30 try to add libevent support
- 2014.3.27 add kqueue support


## Feature

## Not support
- cross platform

## Weakness
- select event loop
- most data structure is linked list
  - cost O(n)
- 内存分配策略简单，来一个生成一个，存在优化空间。
  - 比如改用初始2倍，到达一定数量后，以后每次改用增加额定数量的算法
- 内存占用过大
  - 没有使用数据库，数据都堆在内存里面，可能无法应付大数据量
- 改为单线程版本，降低锁开销，目前锁开销还是非常大的。目测可以改为单进程版本，类似redis，精心维护的话应该能达到不错的效果
- 网络数据读写使用一次尽量多读的方式，避免多次进入系统调用；
- 内存操作优化。不free，留着下次用；
- 考虑使用spwan-fcgi的形式或者内置一次启动多个实例监听同一个端口。这样能更好的发挥机器性能，达到更高的性能；
