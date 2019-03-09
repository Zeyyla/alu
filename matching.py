import multiprocessing

def mp_worker(number):
    number += 1
    return number

def mp_handler():
    p = multiprocessing.Pool(32)
    numbers = list(range(1000))
    with open('results.txt', 'w') as f:
        for result in p.imap_unordered(mp_worker, numbers):
            f.write('%d\n' % result)

if __name__=='__main__':
    mp_handler()