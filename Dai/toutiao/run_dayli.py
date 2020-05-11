import os
import time
import multiprocessing



def toutiao():
    # for i in range(1):
    # os.system('python ./../toutiao/toutiao_sele.py')
    os.system('python ./../toutiao/toutiao_hour_tiyu.py')

def caijing():
    # os.system('python ./../toutiao/toutiao_sele_caijing.py')
    os.system('python ./../toutiao/toutiao_hour_caijing.py')


if __name__ == '__main__':

    pool = multiprocessing.Pool(processes=2)
    pool.apply_async(toutiao)
    pool.apply_async(caijing)
    pool.close()
    pool.join()
