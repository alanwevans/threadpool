threadpool
==========

A simple python threadpool implementation for 2.4+ that can handle KeyboardInterrupt

example
=======
```python
import threadpool

pool = threadool.ThreadPool(5)
for i in range(10):
	pool.add_task(do_something, [i])
```

A more complete example can be found in the module itself, you can use it by running
```python threadpool.py```
