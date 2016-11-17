# 让mysqldump变成并发导出导入的魔法

## 1. 简介
取名mypumpkin，是python封装的一个让mysqldump以多线程的方式导出库表，再以mysql命令多线程导入新库，用于成倍加快导出，特别是导入的速度。这一切只需要在 mysqldump 或 mysql 命令前面加上 `mypumpkin.py` 即可，所以称作魔法。

该程序源于需要对现网单库几百G的数据进行转移到新库，并对中间进行一些特殊操作（如字符集转换），无法容忍mysqldump导入速度。有人可能会提到为什么不用 mydumper，其实也尝试过它但还是放弃了，原因有：

1. 不能设置字符集
mydumper强制使用 binary 方式来连接库以达到不关心备份恢复时的字符集问题，然而我的场景下需要特意以不同的字符集导出、再导入。写这个程序的时候正好在公众号看到网易有推送的一篇文章 ([解密网易MySQL实例迁移高效完成背后的黑科技](http://mp.weixin.qq.com/s?__biz=MzI4NTA1MDEwNg==&mid=2650756926&idx=1&sn=b8081a8ae9456a6051d1ba519febee54&chksm=f3f9e2abc48e6bbd5912edb4e6207ff6ec5bf7123fedbf10b5c65a43146af22845dbf0787b39&scene=0#wechat_redirect))，提到他们对mydumper的改进已支持字符集设置，可是在0.9.1版本的patch里还是没找到。
2. 没有像 mysqldump 那样灵活控制过滤选项（导哪些表、忽略哪些表）
因为数据量之巨大，而且将近70%是不变更的历史表数据，这些表是可以提前导出转换的；又有少量单表大于50G的，最好是分库导出转换。mydumper 不具备 mysqldump 这样的灵活性
3. 对忽略导出gtid信息、触发器等其它支持
阿里云rds 5.6 导出必须要设置 set-gtid-purged=OFF

另外有人还可能提到 mysqlpump —— 它才是我认为mysqldump应该具有的模样，语法兼容，基于表的并发导出。但是只有 mysql服务端 5.7.9 以上才支持，这就是现实和理想的距离。。。

## 2. 实现方法
首先说明，mysqldump的导出速度并不慢，经测试能达到50M/s的速度，10G数据花费3分钟的样子，可以看到瓶颈在于网络和磁盘IO，再怎样的导出工具也快不了多少，但是导入却花了60分钟，磁盘和网络大概只用到了20%，瓶颈在目标库写入速度（而一般顺序写入达不到IOPS限制），所以mypumpkin就诞生了 —— 兼顾myloader的导入速度和mysqldump导出的灵活性。

用python构造1个队列，将需要导出的所有表一次放到队列中，同时启动N个python线程，各自从这个Queue里取出表名，subprocess调用操作系统的mysqldump命令，导出数据到以 dbname.tablename.sql 命名的文件中。load in 与 dump out 类似，根据指定的库名或表名，从dump_dir目录找到所有sql文件，压进队列，N个线程同时调用mysql构造新的命令，模拟 `<` 操作。

参数解析从原来自己解析，到改用argparse模块，几乎做了一次重构。
对于没有指定`--tables`的情况，程序会主动去库里查询一下所有表名，然后过滤进队列。

load in目标库，选项做到与dump out一样丰富，可以指定导入哪些db、哪些表、忽略哪些表。

其中的重点是做到与原mysqldump兼容，因为需要对与表有关的选项（`-B`, `-A`, `--tables`, `--ignore=`），进行分析并组合成新的执行命令，考虑的异常情况非常多。

## 3. 限制
1. **重要**：导出的数据不保证库级别的一致性
  1. 对历史不变表，是不影响的
  2. 具体到一个表能保证一致性，这是mysqldump本身采用哪些选项决定的
  3. 不同表导出动作在不同的mysqldump命令中，无法保证事务。
  在我的案例场景下，是有开发同学辅助使用一套binlog解析程序，等完成后重放所有变更，来保证最终一致性。
  另，许多情况下我们导数据，并不需要完整的或者一致的数据，只是用于离线分析或临时导出，重点是快速拿数据给到开发。
2. 不寻常选项识别
程序已经尽力做到与mysqldump命令兼容，只需要加上 mypumpkin.py、指定dump-dir，就完成并发魔法，但有些情况的参数不方便解析，暂不支持格式：
```
db1 table1 table2
db2 db3
```
即以上无法在命令行下判断 db1、table1 是库名还是表面，用的时候只需记住“[-A|-B], [--tables], [--ignore-table]”三组，必须出现一个：`db1 table1 table2`改成`db1 --tables table1 table2`，`db2`改成`-B db2 db3`。
3. 密码暂只能显式输入

## 4. 使用说明
安装基于python 2.7 开发，其它版本没测。需要按 MySQLdb 库。

### 4.1 help
```
./mypumpkin.py --help
Only mysqldump or mysql allowed after mypumpkin.py

usage: mypumpkin.py {mysqldump|mysqls} [--help]

This's a program that wrap mysqldump/mysql to make them dump-out/load-in
concurrently. Attention: it can not keep consistent for whole database(s).

optional arguments:
  --help                show this help message and exit
  -B db1 [db1 ...], --databases db1 [db1 ...]
                        Dump one or more databases
  -A, --all-databases   Dump all databases
  --tables t1 [t1 ...]  Specifiy tables to dump. Override --databases (-B)
  --ignore-table db1.table1 [db1.table1 ...]
                        Do not dump the specified table. (format like
                        --ignore-table=dbname.tablename). Use the directive
                        multiple times for more than one table to ignore.
  --threads =N          Threads to dump out [2], or load in [CPUs*2].
  --dump-dir DUMP_DIR   Required. Directory to dump out (create if not exist),
                        Or Where to load in sqlfile

At least one of these 3 group options given: [-A,-B] [--tables] [--ignore-table]
```

- `--dump-dir`，必选项，原来用的shell标准输入输出 `> or <` 不允许使用。dump-dir指定目录不存在时会尝试自动创建。
- `--threads=N`，N指定并发导出或导入线程数。dump out 默认线程数2， mypumpkin load in 默认线程数是 cpu个数 * 2。
	注：线程数不是越大越好，这里主要的衡量指标是网络带宽、磁盘IO、目标库IOPS，最好用 dstat 观察一下。
- `-B`, `--tables`，`--ignore-table`，使用与mysqldump相同，如：  
  1. 在mysqldump里面，`--tables`会覆盖`--databases/-B`选项
  2. 在mysqldump里面，`--tables`与`--ignore-table`不能同时出现
  3. 在mysqldump里面，如果没有指定`-B`，则`--tables`或`--ignore-table`必须紧跟db名之后
- 其它选项，mypumpkin会原封不动的保留下来，放到shell去执行。所以如果其它选项有错误，检查是交给原生mysqldump去做的，执行过程遇到一个失败则会退出线程。

### 4.2 example
导出：
```
## 导出源库所有db到visit_dumpdir2目录 （不包括information_schema和performance_schema）
$ ./mypumpkin.py mysqldump -h dbhost_name -utest_user -pyourpassword -P3306 \
 --single-transaction --opt -A --dump-dir visit_dumpdir2

## 导出源库db1,db2，会从原库查询所有表名来过滤
$ ./mypumpkin.py mysqldump -h dbhost_name -utest_user -pyourpassword -P3306 \
 --single-transaction --opt -B db1 db2 --dump-dir visit_dumpdir2

## 只导出db1库的t1,t2表，如果指定表不存在则有提示
$ ./mypumpkin.py mysqldump -h dbhost_name -utest_user -pyourpassword -P3306 \
 --single-transaction --opt -B db1 --tables t1 t2 --dump-dir visit_dumpdir2

## 导出db1,db2库，但忽略 db1.t1, db2.t2, db2.t3表
## mysqldump只支持--ignore-table=db1.t1这种，使用多个重复指令来指定多表。这里做了兼容扩展
$ ./mypumpkin.py mysqldump -h dbhost_name -utest_user -pyourpassword --single-transaction \
 --opt -B db1 db2 --ignore-table=db1.t1 --ignore-table db2.t2 db2.t3 --dump-dir visit_dumpdir2 (如果-A表示全部db)

## 不带 -A/-B
$ ./mypumpkin.py mysqldump -h dbhost_name -utest_user -pyourpassword -P3306 \
 --single-transaction --opt db1 --ignore-table=db1.t1 --dump-dir=visit_dumpdir2

## 其它选项不做处理
$ ./mypumpkin.py mysqldump -h dbhost_name -utest_user -pyourpassword -P3306 \
 --single-transaction --set-gtid-purged=OFF --no-set-names --skip-add-locks -e -q -t -n --skip-triggers \
 --max-allowed-packet=134217728 --net-buffer-length=1638400 --default-character-set=latin1 \
 --insert-ignore --hex-blob --no-autocommit \
 db1 --tables t1 --dump-dir visit_dumpdir2
```

导入：  
`-A`, `-B`, `--tables`, `--ignore-table`, `--threads`, `--dump-dir`用法与作用与上面完全相同，举部分例子：

```
## 导入dump-dir目录下所有表
$ ./mypumpkin.py mysql -h dbhost_name -utest_user -pyourpassword --port 3307 -A \
 --dump-dir=visit_dumpdir2

## 导入db1库（所有表）
$ ./mypumpkin.py mysql -h dbhost_name -utest_user -pyourpassword --port 3307 -B db1 \
 --dump-dir=visit_dumpdir2

## 只导入db.t1表
$ ./mypumpkin.py mysql -h dbhost_name -utest_user -pyourpassword --port 3307 \
 --default-character-set=utf8mb4 --max-allowed-packet=134217728 --net-buffer-length=1638400 \
 -B db1 --tables t1 --dump-dir=visit_dumpdir2

## 导入db1,db2库，但忽略db1.t1表（会到dump-dir目录检查db1,db2有无对应的表存在，不在目标库检查）
$ ./mypumpkin.py mysql -h dbhost_name -utest_user -pyourpassword --port 3307 \
 -B db1 db2 --ignore-table=db1.t1 --dump-dir=visit_dumpdir2
```

## 5.速度对比
