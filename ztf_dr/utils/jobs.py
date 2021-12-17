from os import getpid
from typing import List, Callable, Iterable
from tqdm import tqdm
from multiprocessing import Pool, RLock


def chunker_list(seq: Iterable, size: int):
    return (seq[i::size] for i in range(size))


def run(pid: int, func: Callable, arguments: List) -> List:
    """
    Run a function with some arguments. This use specific progress bar for each pid.
    """
    tqdm_text = f"# {getpid()} {str(pid).zfill(3)}"
    n = len(arguments)
    responses = []
    with tqdm(total=n, desc=tqdm_text, position=pid + 1) as pbar:
        for arg in arguments:
            response = func(*arg)
            responses.append(response)
            pbar.update(1)
    return responses


def run_jobs(arguments: List, func: Callable, num_processes=5) -> List:
    """
    Run a function with n-process. The list of arguments correspond to arguments to the function, these are splitted in
    n-process chunks.
    """
    arguments = chunker_list(arguments, num_processes)
    with Pool(processes=num_processes, initargs=(RLock(),), initializer=tqdm.set_lock) as pool:
        jobs = [pool.apply_async(run, args=(pid, func, arg)) for pid, arg in enumerate(arguments)]
        result_list = [job.get() for job in jobs]
    return sum(result_list, [])
