#coding:utf-8
import sys, os
#from multiprocessing import Queue, Process
from Queue import Queue
import time
import subprocess
import MySQLdb
from threading import Thread
from collections import defaultdict

"""
不改变使用习惯，2点：
    1. 务必带上密码
    2. --result-file
       --result-dir
       >加上转义符号 \>  或者指定--outfile
    3. --tables

--abc=#     必须=
--abc[=#]   可以不用=，若=必须=
--abc=name
"""

queue_out = Queue()
queue_in = Queue()
MYCMD_NEW = []  # handled mysqldump/load


class MyLoad(object):
    def __init__(self, myload_cmd):
        global MYCMD_NEW

        self.myload_cmd = myload_cmd  # not change
        MYCMD_NEW = list(myload_cmd)
        self.dump_dir = self.handle_myload_options()
        self.queue_in = self.queue_myload_tables(myload_cmd)

    def handle_myload_options(self):
        dump_dir = ""
        for myopt in self.myload_cmd:
            if myopt.startswith("--dump-dir="):
                dump_dir = myopt.split("=")[1]
                MYCMD_NEW.remove(myopt)
            elif myopt == "--database" or myopt.startswith("--database="):
                print "you should NOT specify --database option, --databases instead"
                sys.exit(-1)

                # pos_databases = myload_cmd.index(dir_res_may)

        if dump_dir == "":
            print "You must specifiy --dump-dir=xxx. (not support '<')"
            sys.exit(-1)
        elif not os.path.exists(dump_dir):
            print "The specified dump-dir %s does not exist, the program will try to create it for you." % dump_dir
            sys.exit(-1)

        return dump_dir

    def handle_tables_options(self, *myload_cmd):
        """
        --databases db1 db2
        --databases db1 --tables t1 t2
        --databases db1 db2 --ignore-tables db1.t1 db2.t2
        --all-databases [default]
        """
        myload_cmd = myload_cmd[0]
        print "handle_tables_options - myload_cmd: ", myload_cmd

        list_databases = []
        opt_databases = "--databases"
        if opt_databases in myload_cmd:
            pos_databases = myload_cmd.index(opt_databases)
            pos_databases_next = pos_databases + 1
            for db in myload_cmd[pos_databases_next:]:
                if not db.startswith("-"):
                    list_databases.append(db)
                    MYCMD_NEW.remove(db)
                else:
                    break
            if len(list_databases) == 0:
                print "Please give correct database name after --databases "
                sys.exit(-1)
            MYCMD_NEW.remove(opt_databases)
        else:
            list_databases = ""  # --all-databases
        print type(list_databases), list_databases

        opt_tables = "--tables"
        opt_ignore = "--ignore-tables"
        cnt_db = len(list_databases)

        if opt_tables in myload_cmd and cnt_db > 1:
            print "Only one database allowed when --tables given"
            sys.exit(-1)
        if opt_ignore in myload_cmd and opt_tables in myload_cmd:
            print "Error: you should NOT specifiy --ignore-tables and --tables both"
            sys.exit(-1)

        dict_tables_os = defaultdict(list)
        for dirName, subdirList, fileList in os.walk(self.dump_dir):
            for fname in fileList:
                fname_list = fname.split(".")
                if fname_list[-1] == "sql":
                    schema_name, table_name = fname_list[0], fname_list[1]
                    #schema_table_name = schema_name + "." + table_name
                    dict_tables_os[schema_name].append(fname)
        print "dict_table_os: ", dict_tables_os

        set_db_notdump = set(list_databases) - set(dict_tables_os.keys())
        if len(set_db_notdump) > 0:
            print "You have specified database that have not been dumped: ", set_db_notdump
            sys.exit(-1)
        else:
            if list_databases == "":
                list_databases = dict_tables_os.keys()  # all databases dumped
            set_db_notload = set(dict_tables_os.keys()) - set(list_databases)
            print "os list: ", dict_tables_os.keys(), list_databases
            for db in set_db_notload:
                del dict_tables_os[db]
            print "dict_table_os after databases: ", dict_tables_os

        if opt_ignore in myload_cmd:
            pos_ignore = myload_cmd.index(opt_ignore)
            pos_ignore_next = pos_ignore + 1
            for ignore_table in myload_cmd[pos_ignore_next:]:
                if not ignore_table.startswith("-"):
                    try:
                        db_name, tb_name = ignore_table.split(".")
                    except ValueError, e:
                        print "ignore-tables must be specified like dbname.tablename"
                        sys.exit(-1)
                    try:
                        dict_tables_os[db_name].remove(ignore_table + ".sql")  # table not dumped exeption
                    except ValueError, e:
                        print "Table %s dump file can not be reached." % ignore_table
                        sys.exit(-1)
                    MYCMD_NEW.remove(ignore_table)
                else:
                    break
            MYCMD_NEW.remove(opt_ignore)
            print "dict_table_os after ignored: ", dict_tables_os

        elif opt_tables in myload_cmd and cnt_db == 1:
            include_db = list_databases[0]
            pos_tables = myload_cmd.index(opt_tables)
            pos_tables_next = pos_tables + 1
            list_include_tables = []
            for include_table in myload_cmd[pos_tables_next:]:
                if not include_table.startswith("-"):
                    schema_table_sql = "%s.%s.sql" % (include_db, include_table)
                    list_include_tables.append(schema_table_sql)
                    MYCMD_NEW.remove(include_table)
                else:
                    break
            MYCMD_NEW.remove(opt_tables)

            for tab in list_include_tables:
                if tab not in dict_tables_os[include_db]:
                    print "You have specified table that have not been dumped: ", tab
                    sys.exit(-1)
            dict_tables_os.clear()
            dict_tables_os[include_db] = list_include_tables  # rewrite include tables to list_os
            print "dict_table_os after tabled: ", dict_tables_os

        # else just --databases or nothing
        print "handled table options: MYCMD_NEW: ",MYCMD_NEW
        return dict_tables_os

    def queue_myload_tables(self, *myload_cmd):
        # myload_cmd_new = list(self.myload_cmd)
        myload_cmd = myload_cmd[0]
        print "queue_myload_tables - myload_cmd: ", myload_cmd
        tab_file_queue = Queue()

        # process --databases --tables --ignore-table= option
        tables_dict = self.handle_tables_options(myload_cmd)
        print "table_dict: ", tables_dict

        for db, tabs in tables_dict.items():
            for tab in tabs:
                tab_file_queue.put(tab)

        print "tables dict queue done"
        return tab_file_queue

    # load one table from dumpdir into database
    # get sql file list from queue_in
    # def load_in(self):
    def do_process(self):
        while True:
            if not self.queue_in.empty():
                in_table = self.queue_in.get(block=False)
                in_table_list = in_table.split(".")
                schema_name, table_name = in_table_list[0], in_table_list[1]

                load_option = " --database %s < %s/%s" % (schema_name, self.dump_dir, in_table)
                myload_cmd_run = " ".join(MYCMD_NEW) + load_option
                print "myload_cmd_run: ", myload_cmd_run
                # subprocess.call(myload_cmd_run, shell=True)
                time.sleep(1)
            else:
                print "load over"
                break


class MyDump(object):
    def __init__(self, mydump_cmd):
        global MYCMD_NEW
        global MYQUEUE

        self.mydump_cmd = mydump_cmd  # not change
        MYCMD_NEW = list(mydump_cmd)
        self.dump_dir = self.handle_mydump_options()
        MYQUEUE = self.queue_mydump_tables(mydump_cmd)

    def handle_mydump_options(self):
        dump_dir = ""
        for myopt in self.mydump_cmd:
            if myopt.startswith("--dump-dir="):
                dump_dir = myopt.split("=")[1]
                MYCMD_NEW.remove(myopt)
                # pos_databases = myload_cmd.index(dir_res_may)

        if dump_dir == "":
            print "You must specifiy --dump-dir=xxx. (not support '>')"
            sys.exit(-1)
        elif not os.path.exists(dump_dir):
            print "The specified dump-dir %s does not exist, the program will try to create it for you." % dump_dir
            try:
                os.makedirs(dump_dir)
            except:
                print "创建目录 %s 失败" % dump_dir
                sys.exit(-1)

        return dump_dir

    def handle_tables_options(self, *mydump_cmd):
        print type(mydump_cmd), mydump_cmd
        mydump_cmd = mydump_cmd[0]

        opt_tables = "--tables"
        opt_databases = "-B"
        opt_ignore = "--ignore-table="
        db_tables = defaultdict(list)

        if opt_tables in mydump_cmd:
            pos_tables = mydump_cmd.index(opt_tables)
            dbname = mydump_cmd[pos_tables - 1]
            print "--tables pos: ", pos_tables
            print "database: ", dbname
            print "table names:"

            pos_tables_next = pos_tables + 1
            list_tables = []
            # while pos_tables_next < len(MYCMD_NEW):
            for tab in mydump_cmd[pos_tables_next:]:
                if not tab.startswith("-"):
                    print tab
                    list_tables.append(tab)
                    MYCMD_NEW.remove(tab)
                    #pos_tables_next += 1
                else:
                    break

            MYCMD_NEW.remove(dbname)
            MYCMD_NEW.remove(opt_tables)
            print "MYCMD_NEW_handled:", MYCMD_NEW

            db_tables[dbname] = list_tables

        else:
            db_tables = self.get_tables_from_db()

            if opt_databases in mydump_cmd:
                pos_databases = mydump_cmd.index(opt_databases)
                pos_databases_next = pos_databases + 1
                list_databases = []
                #while pos_databases_next < len(MYCMD_NEW):  # len change
                for db in mydump_cmd[pos_databases_next:]:
                    # db = MYCMD_NEW[pos_databases_next]
                    if not db.startswith("-"):
                        list_databases.append(db)
                        MYCMD_NEW.remove(db)
                    else:
                        break
                MYCMD_NEW.remove(opt_databases)
            else:
                list_databases = ""
            print "list_database from args", type(list_databases), list_databases

            set_db_notdump = set(list_databases) - set(db_tables.keys())
            if len(set_db_notdump) > 0:
                print "You have specified database that do not exist: ", set_db_notdump
                sys.exit(-1)
            else:
                if list_databases == "":
                    list_databases = db_tables.keys()  # all databases dumped
                set_db_notload = set(db_tables.keys()) - set(list_databases)
                print "os list: ", db_tables.keys(), list_databases
                for db in set_db_notload:
                    del db_tables[db]
                print "dict_table_os after databases: ", db_tables

            #if opt_ignore in mydump_cmd:
            #    pos_ignore = mydump_cmd.index(opt_ignore)
                #pos_ignore_next = pos_ignore + 1
            for ignore_table in mydump_cmd:
                if ignore_table.startswith(opt_ignore):

                    try:
                        db_name, tb_name = ignore_table.split("=")[1].split(".")
                    except ValueError:
                        print "ignore-tables must be specified like dbname.tablename"
                        sys.exit(-1)
                    try:
                        db_tables[db_name].remove(tb_name)  # table not dumped exeption
                        MYCMD_NEW.remove(ignore_table)
                    except ValueError:
                        print "Table %s does not exist." % ignore_table
                        sys.exit(-1)

                # MYCMD_NEW.remove(opt_ignore)

        print "MYCMD_NEW_handled - database :", MYCMD_NEW
        print "final tables to dump:", db_tables

        return db_tables
            # print "queue_out qsize ", queue_out.qsize()

    def queue_mydump_tables(self, *mydump_cmd):
        mydump_cmd = mydump_cmd[0]
        print "queue_mydump_tables - mydump_cmd: ", mydump_cmd
        tab_file_queue = Queue()

        # process --databases --tables --ignore-table= option
        tables_dict = self.handle_tables_options(mydump_cmd)
        # print "table_dict: ", tables_dict

        for db, tabs in tables_dict.items():
            for tab in tabs:
                tab_file_queue.put("{0}.{1}".format(db, tab))

        print "tables dict queue done"
        return tab_file_queue

    def get_tables_from_db(self):
        dbinfo = self.get_conninfo_from_cmd()
        print "dbinfo:", dbinfo
        conn = MySQLdb.Connect(host=dbinfo[0], user=dbinfo[1], passwd=dbinfo[2], port=dbinfo[3], connect_timeout=5)
        cur = conn.cursor()

        sqlstr = "select table_schema, table_name from information_schema.tables where TABLE_TYPE = 'BASE TABLE' AND " \
                 "TABLE_SCHEMA not in('information_schema', 'performance_schema', 'sys')"
        print "get tables:", sqlstr
        cur.execute(sqlstr)
        res = cur.fetchall()
        cur.close()
        conn.close()

        dict_tables_db = defaultdict(list)
        for d, t in res:
            dict_tables_db[d].append(t)

        # print "db all tables: ", dict_tables_db
        return dict_tables_db

    def get_conninfo_from_cmd(self):
        db_host = ""
        db_user = ""
        db_pass = ""
        db_port = 0

        mydump_cmd = self.mydump_cmd

        for db_args in mydump_cmd:
            if db_args.startswith("-h"):
                db_host_idx = mydump_cmd.index(db_args)
                if db_args == "-h":
                    db_host = mydump_cmd[db_host_idx + 1]
                else:
                    db_host = mydump_cmd[db_host_idx][2:]
            elif db_args.startswith("-u"):
                db_user_idx = mydump_cmd.index(db_args)
                if db_args == "-u":
                    db_user = mydump_cmd[db_user_idx + 1]
                else:
                    db_user = mydump_cmd[db_user_idx][2:]
            elif db_args.startswith("-p"):
                db_pass_idx = mydump_cmd.index(db_args)
                db_pass = mydump_cmd[db_pass_idx][2:]
            elif db_args.startswith("-P"):
                print "db_port, args", db_args
                db_port_idx = mydump_cmd.index(db_args)
                db_port = int(mydump_cmd[db_port_idx][2:])

            if db_host != "" and db_user != "" and db_pass != "" and db_port != 0:
                break

        if db_host == "" or db_user == "" or db_pass == "":
            print "wrong db connect info given"
            sys.exit(-1)
        elif db_port == 0:
            db_port = 3306
        return db_host, db_user, db_pass, db_port

    # def dump_out(self):
    def do_process(self):
        global MYQUEUE
        while True:
            if not MYQUEUE.empty():
                in_table = MYQUEUE.get(block=False)
                in_table_list = in_table.split(".")
                schema_name, table_name = in_table_list[0], in_table_list[1]

                dump_option = " %s --tables %s --result-file=%s/%s.sql" \
                              % (schema_name, table_name, self.dump_dir, in_table)
                mydump_cmd_run = " ".join(MYCMD_NEW) + dump_option
                print "mydump_cmd_run: ", mydump_cmd_run
                # subprocess.call(mydump_cmd_run, shell=True)
                time.sleep(1)
            else:
                print "dump over"
                break


class myThread(Thread):
    def __init__(self, myprocess):
        Thread.__init__(self)
        self.myprocess = myprocess

    def run(self):
        # 消费线程不关心队列里是哪个表的sql
        self.myprocess.do_process()


MYQUEUE = Queue()

if __name__ == '__main__':
    global MYQUEUE
    #python mysql_mload.py mysqldump -uecuser -p strongpassword -P3306 -h 10.0.200.195 -B d_ec_crm t1 t2

    myload_cmd = ['mysql_mload.py', 'mysql', '-h', '10.0.200.195', '-u', 'ecuser', '-pecuser', '-P3307', '--default-character-set=utf8mb4',
              # '--databases', 'd_ec_crm',  'd_ec_crmextend',
              '--ignore-tables', 'd_ec_crm.t_eccrm_detail',  # 'd_ec_crm.t_crm_contact_at201610',  # 'd_ec.t_eccrm_detail',
              '--dump-dir=dumpdir']
    """
    mydump_cmd = ['mysql_mload.py', 'mysqldump', '-h', '192.168.1.125', '-u', 'ecuser', '-pecuser', '-P3308',
                     '--single-transaction', '--no-set-names', '--skip-add-locks', '-e', '-q', '-t', '-n', '--skip-triggers', '--no-autocommit', '--max-allowed-packet=134217728', '--net-buffer-length=1638400',
                     '--hex-blob', '--default-character-set=latin1', '--dump-dir=dumpdir',
                     'd_ec_crm0', '--tables', 't_crm_qq_record_201612', 't_crm_qq_record_201611', 't_crm_qq_record_201610', 't_crm_qq_record_201609',
                     't_crm_qq_record_201608', 't_crm_qq_record_201607', 't_crm_qq_record_201606', 't_crm_qq_record_201509', 't_crm_qq_record_201610']
                     # '-B', "d_ec_crm0", "d_ec_crm1", "--ignore-table=d_ec_crm0.t_crm_qq_record_201612",
                     # '--ignore-table=d_ec_crm1.t_crm_qq_record_201611']
    """
    mycmd_wrap = myload_cmd # sys.argv
    mycmd = mycmd_wrap[1:]
    dump_load = mycmd_wrap[1]

    print mycmd
    # my_process = None
    if dump_load == 'mysqldump':
        my_process = MyDump(mycmd)
    elif dump_load == 'mysql':
        my_process = MyLoad(mycmd)
    else:
        print "mysqldump / mysql"
        sys.exit(-1)

    # myqueue = Queue()
    # mydump = MyDump(mydump_cmd[1:])
    # MYQUEUE = mydump.queue_mydump_tables(mydump_cmd)

    for i in range(2):
        worker = myThread(my_process)
        # worker.setDaemon(True)
        worker.start()
