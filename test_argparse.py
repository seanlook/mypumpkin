import sys
import getopt
import argparse
from multiprocessing import cpu_count

mydump_cmd = ['mysql_mload.py', 'mysqldump', '-h', '192.168.1.125', '-u', 'ecuser', '-pecuser', '-P3308',
                     '--single-transaction', '--no-set-names', '--skip-add-locks', '-e', '-q', '-t', '-n', '--skip-triggers', '--no-autocommit', '--max-allowed-packet=134217728', '--net-buffer-length=1638400',
                     '--hex-blob', '--default-character-set=latin1', '--dump-dir=dumpdir2',
                     'd_ec_crm0', '--tables', 't_crm_qq_record_201612', 't_crm_qq_record_201611', 't_crm_qq_record_201610', 't_crm_qq_record_201609',
                     't_crm_qq_record_201608', 't_crm_qq_record_201607', 't_crm_qq_record_201606', 't_crm_qq_record_201509', 't_crm_qq_record_201610',
                     '--ignore-table=a.b', '--ignore-table=a.c']
                     # '-B', "d_ec_crm0", "d_ec_crm1", "--ignore-table=d_ec_crm0.t_crm_qq_record_201612",
                     # '--ignore-table=d_ec_crm1.t_crm_qq_record_201611']

options = []
for o in mydump_cmd:
    pass

mydump_cmd = ['mysql_mload.py', 'mysqldump', "--dump-dir=dumpdir",  # '--dump-dir', 'dumpdir2',
                     '--tables', 't_crm_qq_record_201612', 't_crm_qq_record_201611', 't_crm_qq_record_201610', 't_crm_qq_record_201609',
                     't_crm_qq_record_201608', 't_crm_qq_record_201607', 't_crm_qq_record_201606', 't_crm_qq_record_201509', 't_crm_qq_record_201610',
                     # '--ignore-table=a.b', '--ignore-table=a.c', '-h100.0.200.195'
                     ]


args_short = 'B:o:'
args_long = ['databases=', 'dump-dir=', 'tables=', 'ignore-table=']

opts, args = getopt.getopt(mydump_cmd[2:], args_short, args_long)

for opt, val in opts:
    pass #print opt, val
    #if opt in ('-B', '--databases'):
    #    print val
# print "opt over"
# print args



parser = argparse.ArgumentParser(description="Process some args", conflict_handler='resolve')

group = parser.add_mutually_exclusive_group()

print "num of cpus", cpu_count()
num_threads = cpu_count() * 2

parser.add_argument('-B', '--databases', nargs='+', help='mysqldump --databases')
parser.add_argument('-h', '--host', nargs=1, help='host name mysql')
group.add_argument('--tables', nargs='+', action='store')
group.add_argument('--ignore-table', nargs=1, action='append')
group.add_argument('--threads', nargs=1, default=num_threads, type=int)
parser.add_argument('--dump-dir', nargs=1, required=True, action='store', help='mysqldump out dir')

print parser.parse_args(mydump_cmd[2:])
