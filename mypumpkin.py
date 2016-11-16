#!/usr/bin/python
#coding:utf-8

import sys, os
from Queue import Queue
import time
import subprocess
import MySQLdb
from threading import Thread
from collections import defaultdict
# import argparse
from argparse import ArgumentParser
from multiprocessing import cpu_count

MYCMD_NEW = []  # handled mysqldump/load
MYQUEUE = Queue()


class NewOptions(object):
    def __init__(self, mycmd):
        global MYCMD_NEW
        self.mycmd = mycmd

        work_mode = ''
        try:
            if mycmd[1] == 'mysqldump':
                work_mode = 'DUMP'
            elif mycmd[1] == 'mysql':
                work_mode = 'LOAD'
            else:
                print "Only mysqldump or mysql allowed after mypumpkin.py\n"
                # myparser will do the next
        except IndexError:
            pass
            #help_parser = self.parse_myopt()
            #ArgumentParser.error(help_parser, help_parser.print_help())

        myparser = self.parse_myopt(work_mode)

        self.myopts, MYCMD_NEW = myparser.parse_known_args(mycmd)
        print "myparse options handling tables&dbs: ", self.myopts

        self.threads = self.myopts.threads[0]
        self.dumpdir = self.get_dumpdir(work_mode)

    def get_dumpdir(self, work_mode):
        dump_dir = self.myopts.dump_dir[0]
        if dump_dir == "":
            print "You must specifiy --dump-dir=xxx. (not support '>')"
            sys.exit(-1)
        elif not os.path.exists(dump_dir):
            if work_mode == 'DUMP':
                print "The specified dump-dir %s does not exist, the program will try to create it for you." % dump_dir
                try:
                    os.makedirs(dump_dir)
                except:
                    print "创建目录 %s 失败" % dump_dir
                    sys.exit(-1)
            elif work_mode == 'LOAD':
                print "The specified dump-dir %s does not exist"
                sys.exit(-1)
        return dump_dir

    def parse_myopt(self, work_mode=''):
        parser = ArgumentParser(description="This's a program that wrap mysqldump/mysql to make them dump-out/load-in concurrently.\n"
                                            "Attention: it can not keep consistent for whole database(s).",
                                add_help=False,
                                usage='%(prog)s {mysqldump|mysqls} [--help]',
                                epilog="At least one of these 3 group options given: [-A,-B] [--tables] [--ignore-table]")  # , allow_abbrev=False)
        group1 = parser.add_mutually_exclusive_group()
        group2 = parser.add_mutually_exclusive_group()
        # group_dbinfo = parser.add_argument_group('db connect info')

        num_threads = cpu_count() * 2
        if work_mode == 'DUMP':
            num_threads = 2

        # parser.add_argument('mysql_cmd', choices=['mysqldump', 'mysql'])
        parser.add_argument('--help', action='help', help='show this help message and exit')

        group1.add_argument('-B', '--databases', nargs='+', metavar='db1', help='Dump one or more databases')
        group1.add_argument('-A', '--all-databases', action='store_true', help='Dump all databases')
        group2.add_argument('--tables', nargs='+', metavar='t1',
                            help='Specifiy tables to dump. Override --databases (-B)')
        group2.add_argument('--ignore-table', nargs='+', metavar='db1.table1', action='append',
                            help='Do not dump the specified table. (format like --ignore-table=dbname.tablename). '
                                 'Use the directive multiple times for more than one table to ignore.')
        parser.add_argument('--threads', nargs=1, metavar='=N', default=[num_threads], type=int, help='Threads to dump out [2], or load in [CPUs*2].')
        parser.add_argument('--dump-dir', nargs=1, required=True, action='store', help='Required. Directory to dump out (create if not exist), Or Where to load in sqlfile')

        # print parser.parse_args(mydump_cmd[2:])
        return parser  # .parse_args()

    def get_tables_opt(self):
        global MYCMD_NEW

        print "Start to handle your table relevant options..."
        opt_dbs = self.myopts.databases
        opt_is_alldbs = self.myopts.all_databases
        opt_tables = self.myopts.tables
        opt_ignores = self.myopts.ignore_table

        len_dbs = [len(opt_dbs) if opt_dbs is not None else 0][0]
        len_alldbs = [1 if opt_is_alldbs else 0][0]
        len_tables = [len(opt_tables) if opt_tables is not None else 0][0]
        len_ignores = [len(opt_ignores) if opt_ignores is not None else 0][0]


        """ 5种情形
        1. -B db1 db2  或者 -A
        2. -B db1 --table t1 t2
        3. -B db1 db2 --ignore-table db1.t1 db1.t2 --ignore-table db2.t1 db2.t2  或者 -A --ignore...
        4. db1 --ignore-table=db1.t1 --ignore-table=db1.t2
        5. db1 --tables t1 t2

        db1 t1 t2  not support
        db1 not support
        --tables与-B与--ignore-table必出现其一
        --tables与--ignore-table只能出现其一
        -A,-B只能出现其一
        --tables, --ignore-table 必紧跟隐式db之后
        """

        if len_tables + len_ignores + len_dbs + len_alldbs == 0:
            print "Error: at least one of [--tables, --ignore-table, -B, -A] is specified!"
            sys.exit(-1)

        tables_handler = []  # --tables, --ignore-table, --B d1 d2    dbname.*
        dbname_list = []
        tables_tag = 'db-include'  # ignore-table  databases  all-databases

        if (len_alldbs > 0 or len_dbs > 1) and len_tables > 0:
            print "Error: --tables only be specified with one databases"
            sys.exit(-1)
        elif len_dbs + len_alldbs == 0:  # 情形4和5，没有显示指定db
            for table_opt in self.mycmd:
                if table_opt.startswith('--tables') or table_opt.startswith('--ignore-table'):
                    pos_table_opt = self.mycmd.index(table_opt)
                    pos_dbname = pos_table_opt - 1
                    dbname = self.mycmd[pos_dbname]

                    if dbname.startswith('-'):
                        print "Error: Please give the right database name"
                        sys.exit(-1)
                    else:
                        dbname_list = [dbname]
                        MYCMD_NEW.remove(dbname)

                    break
        else:
            # tables_tag = 'include'
            if opt_dbs is not None:
                dbname_list = opt_dbs
            elif opt_is_alldbs:
                dbname_list = []
            else:
                print "no right databases given. this should never be print"
        print "mypumpkin>> This is the databases detected: ", dbname_list

        if opt_tables is not None:  # 情景5，2
            for tab in opt_tables:
                tables_handler.append(dbname_list[0] + "." + tab)
            tables_tag = 'include-tab'
        elif opt_ignores is not None:  # 情景4，3
            for tabs in opt_ignores:
                for db_tab in tabs:
                    tables_handler.append(db_tab)
            tables_tag = 'db-exclude'
        print "mypumpkin>> This is the tables (%s) detected: %s" %(tables_tag, tables_handler)

        MYCMD_NEW = MYCMD_NEW[1:]  # 去掉外包装
        # print "MYCMD_NEW ready:", MYCMD_NEW
        return dbname_list, tables_handler, tables_tag


class MyLoad(NewOptions):

    def handle_tables_options(self):
        dbname_list, tables_list, tables_tag = self.get_tables_opt()

        all_tables_os = defaultdict(list)
        for dirName, subdirList, fileList in os.walk(self.dumpdir):
            for fname in fileList:
                fname_list = fname.split(".")
                if fname_list[-1] == "sql":
                    schema_name, table_name = fname_list[0], fname_list[1]
                    all_tables_os[schema_name].append(table_name)
        # print "all_tables_os: ", all_tables_os

        if tables_tag == 'include-tab':  # [-B] db1 --table t1
            all_tables = defaultdict(list)
            for st_name in tables_list:
                db_name, tb_name = st_name.split(".")
                if tb_name in all_tables_os[db_name]:
                    all_tables[db_name].append(tb_name)
                else:
                    print "Error: can not find dumped file for table [%s]" % st_name
                    sys.exit(-1)
            all_tables_os = all_tables  # include
        elif tables_tag.startswith('db-'):  # -B db1 db2 (-A)
            # all_tables = self.get_tables_from_db()  # 从db里面获取所有表
            if len(dbname_list) != 0:  # not -A
                set_db_notexist = set(dbname_list) - set(all_tables_os.keys())
                if set_db_notexist:
                    print "Error: Db [%s] do not dumped" % ",".join(set_db_notexist)
                    sys.exit(-1)
                for db_l in all_tables_os.keys():
                    if db_l not in dbname_list:
                        del all_tables_os[db_l]  # 删除不在-B指定的db

            if tables_tag == 'db-exclude':  # db1 --ignore-table db1.t1,  -B db1 [db2] --ignore-table (-A)
                for st_name in tables_list:
                    db_name, tb_name = st_name.split(".")
                    try:
                        all_tables_os[db_name].remove(tb_name)
                    except ValueError:
                        print "Error: can not get ignored table [%s] from dumped directory [%s] " % (st_name, self.dumpdir)
                        sys.exit(-1)

        return all_tables_os

    def queue_myload_tables(self):
        global MYQUEUE

        tables_dict = self.handle_tables_options()
        # print "Tables to load: ", tables_dict

        for db, tabs in tables_dict.items():
            for tab in tabs:
                MYQUEUE.put("{0}.{1}".format(db, tab))

        print "mypumpkin>> tables waiting to load in have queued"

    # load one table from dumpdir into database
    # get sql file list from queue_in
    # def load_in(self):
    def do_process(self):
        global MYQUEUE
        while True:
            if not MYQUEUE.empty():
                in_table = MYQUEUE.get(block=False)
                in_table_list = in_table.split(".")
                schema_name, table_name = in_table_list[0], in_table_list[1]

                load_option = " --database %s < %s/%s.sql" % (schema_name, self.dumpdir, in_table)
                myload_cmd_run = " ".join(MYCMD_NEW) + load_option
                try:
                    print "mypumpkin>> Loading in table [%s]: " % in_table
                    print "  " + myload_cmd_run
                    subprocess.check_output(myload_cmd_run, shell=True)  # , stderr=subprocess.STDOUT)
                    # 进程的输出，包括warning和错误，都打印出来
                except subprocess.CalledProcessError as e:
                    print "Error shell returncode %d: exit \n" % e.returncode
                    sys.exit(-1)
                time.sleep(0.3)
            else:
                print "mypumpkin>> databases and tables load thread finished"
                break


class MyDump(NewOptions):

    def handle_tables_options(self):
        dbname_list, tables_list, tables_tag = self.get_tables_opt()

        all_tables = defaultdict(list)
        if tables_tag == 'include-tab':  # [-B] db1 --table t1
            for st_name in tables_list:
                db_name, tb_name = st_name.split(".")
                all_tables[db_name].append(tb_name)
        elif tables_tag.startswith('db-'):  # -B db1 db2 (-A)
            all_tables = self.get_tables_from_db()  # 从db里面获取所有表
            if len(dbname_list) != 0:  # not -A
                set_db_notexist = set(dbname_list) - set(all_tables.keys())
                if set_db_notexist:
                    print "Error: Db [%s] do not exist" % ",".join(set_db_notexist)
                    sys.exit(-1)
                for db_l in all_tables.keys():
                    if db_l not in dbname_list:
                        del all_tables[db_l]

            if tables_tag == 'db-exclude':  # db1 --ignore-table db1.t1,  -B db1 [db2] --ignore-table (-A)
                for st_name in tables_list:
                    db_name, tb_name = st_name.split(".")
                    try:
                        all_tables[db_name].remove(tb_name)
                    except ValueError:
                        print "Table %s does not exist (or not in -B databases)." % st_name
                        sys.exit(-1)

        return all_tables

    def queue_mydump_tables(self):
        global MYQUEUE

        tables_dict = self.handle_tables_options()
        # print "table_dict: ", tables_dict

        for db, tabs in tables_dict.items():
            for tab in tabs:
                MYQUEUE.put("{0}.{1}".format(db, tab))

        print "mypumpkin>> tables waiting to dump out have queued"

    def get_tables_from_db(self):
        print "Go for target db to get all tables list..."

        dbinfo = self.get_dbinfo_cmd()

        try:
            if dbinfo[4] is not None:  # socket given
                conn = MySQLdb.Connect(host=dbinfo[0], user=dbinfo[1], passwd=dbinfo[2], port=dbinfo[3],
                                       unix_socket=dbinfo[4], connect_timeout=5)
            else:
                conn = MySQLdb.Connect(host=dbinfo[0], user=dbinfo[1], passwd=dbinfo[2], port=dbinfo[3], connect_timeout=5)
            cur = conn.cursor()

            sqlstr = "select table_schema, table_name from information_schema.tables where TABLE_TYPE = 'BASE TABLE' AND " \
                     "TABLE_SCHEMA not in('information_schema', 'performance_schema', 'sys')"
            # print "get tables:", sqlstr
            cur.execute(sqlstr)
        except MySQLdb.Error, e:
            print "Error mysql %d: %s" % (e.args[0], e.args[1])
            sys.exit(-1)

        res = cur.fetchall()
        cur.close()
        conn.close()

        dict_tables_db = defaultdict(list)
        for d, t in res:
            dict_tables_db[d].append(t)

        # print "db all tables: ", dict_tables_db
        return dict_tables_db

    def get_dbinfo_cmd(self):
        parser = ArgumentParser(description="Process some args", conflict_handler='resolve')

        parser.add_argument('-h', '--host', nargs=1, metavar='host1', help='Host to connect')
        parser.add_argument('-u', '--user', nargs=1, metavar='user1', help='User to connect')
        parser.add_argument('-p', '--password', nargs=1, metavar='yourpassword', help='Password for user1 to connect')
        parser.add_argument('-P', '--port', nargs=1, metavar='port', type=int, default=3306, help='Port for host to connect')
        parser.add_argument('-S', '--socket', nargs=1, metavar='socket', help='Socket address for host to connect')

        dbinfo_opt, _ = parser.parse_known_args(self.mycmd)

        db_host = dbinfo_opt.host[0]
        db_user = dbinfo_opt.user[0]
        db_pass = dbinfo_opt.password[0]
        db_port = dbinfo_opt.port[0]
        db_sock = dbinfo_opt.socket

        return db_host, db_user, db_pass, db_port, db_sock

    # def dump_out(self):
    def do_process(self):
        global MYQUEUE
        while True:
            if not MYQUEUE.empty():
                in_table = MYQUEUE.get(block=False)
                in_table_list = in_table.split(".")
                schema_name, table_name = in_table_list[0], in_table_list[1]

                dump_option = " %s --tables %s --result-file=%s/%s.sql" \
                              % (schema_name, table_name, self.dumpdir, in_table)
                mydump_cmd_run = " ".join(MYCMD_NEW) + dump_option

                try:
                    print "mypumpkin>> Dumping out table [%s]: " % in_table
                    print "  " + mydump_cmd_run
                    subprocess.check_output(mydump_cmd_run, shell=True)  # , stderr=subprocess.STDOUT)
                    # 进程的输出，包括warning和错误，都打印出来
                except subprocess.CalledProcessError as e:
                    print "Error shell returncode %d: exit \n" % e.returncode
                    sys.exit(-1)
                time.sleep(0.3)
            else:
                print "mypumpkin>> databases and tables dump thread finished"
                break


class myThread(Thread):
    def __init__(self, myprocess):
        Thread.__init__(self)
        self.myprocess = myprocess

    def run(self):
        # 消费线程不关心队列里是哪个表的sql
        self.myprocess.do_process()


if __name__ == '__main__':
    #python mypumpkin.py mysqldump -uecuser -p strongpassword -P3306 -h 10.0.200.195 -B d_ec_crm t1 t2

    myload_cmd = ['mypumpkin.py', 'mysql', '-h', '10.0.200.195', '-u', 'ecuser', '-pecuser', '-P3307', '--default-character-set=utf8mb4',
              '--databases', 'd_ec_crm',  'd_ec_crmextend',  # '--tables', 't2',
              '--ignore-table', 'd_ec_crm.t_eccrm_detail',  # 'd_ec_crm.t_crm_contact_at201610',  # 'd_ec.t_eccrm_detail',
              '--dump-dir=dumpdir']

    mydump_cmd = ['mypumpkin.py', 'mysqldump', '-h', '192.168.1.125', '-u', 'ecuser', '-pecuser', '-P3308',
                     '--single-transaction', '--no-set-names', '--skip-add-locks', '-e', '-q', '-t', '-n', '--skip-triggers', '--no-autocommit', '--max-allowed-packet=134217728', '--net-buffer-length=1638400',
                     '--hex-blob', '--default-character-set=latin1', '--dump-dir=dumpdir',  #'--ignore-table', 'dd',
                     '-B', 'd_ec_crm0' , # 'd_ec_crm1',
                     '--tables', 't_crm_qq_record_201612', 't_crm_qq_record_201611', 't_crm_qq_record_201610', 't_crm_qq_record_201609',
                     # 't_crm_qq_record_201608', 't_crm_qq_record_201607', 't_crm_qq_record_201606', 't_crm_qq_record_201509', 't_crm_qq_record_201610'
                     ]
                     # '-B', "d_ec_crm0", "d_ec_crm1", "--ignore-table=d_ec_crm0.t_crm_qq_record_201612",
                     # '--ignore-table=d_ec_crm1.t_crm_qq_record_201611', '--ignore-table=d_ec_crm1.t_crm_qq_record_201610', '--ignore-table=d_ec_crm1.t_crm_qq_record_201608',
                     # '--ignore-table', 'd_ec_crm0.t_crm_qq_record_201611', 'd_ec_crm0.t_crm_qq_record_201607']

    mycmd = sys.argv
    my_process = NewOptions(mycmd)  # just for args check
    my_process = None

    # print mycmd
    # my_process = None
    if mycmd[1] == 'mysqldump':
        my_process = MyDump(mycmd)
        my_process.queue_mydump_tables()
    elif mycmd[1] == 'mysql':
        my_process = MyLoad(mycmd)
        my_process.queue_myload_tables()
    else:
        print "Only mysqldump or mysql allowed after mypumpkin.py\n"  # should never print
        sys.exit(-1)

    num_threads = my_process.threads

    print "mypumpkin>> number of threads: ", num_threads
    for i in range(num_threads):
        worker = myThread(my_process)
        # worker.setDaemon(True)
        worker.start()
        time.sleep(0.5)
