from collections import defaultdict
import threading


# Map函数
def map_function(data_chunk):
    """
    对数据块进行处理，返回乘客ID和飞行次数的映射。
    """
    result = defaultdict(int)
    for line in data_chunk:
        if line.strip() == '':
            continue
        passenger_id = line.split(',')[0]  # 假设乘客ID是CSV文件中的第一列
        result[passenger_id] += 1
    return result


# Reduce函数
def reduce_function(mapped_data):
    """
    合并Map函数的结果，找出飞行次数最多的乘客。
    """
    result = defaultdict(int)
    for data in mapped_data:
        for passenger_id, count in data.items():
            result[passenger_id] += count
    return result


# 读取数据并分块处理
def read_data(file_path):
    """
    读取CSV文件，按块返回数据。
    """
    with open(file_path, 'r') as file:
        while True:
            lines = [file.readline() for _ in range(1000)]  # 每次读取1000行
            if not lines or lines[0] == '':
                break
            yield lines


# 多线程执行Map函数
def execute_map(data_chunks):
    """
    使用多线程执行Map函数。
    """
    threads = []
    mapped_results = []
    for chunk in data_chunks:
        thread = threading.Thread(target=lambda q, arg1: q.append(map_function(arg1)), args=(mapped_results, chunk))
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()
    return mapped_results


# 主程序
if __name__ == '__main__':
    data_chunks = read_data('AComp_Passenger_data_no_error.csv')  # 假设CSV文件名为AComp_Passenger_data_no_error.csv
    mapped_results = execute_map(data_chunks)
    reduced_result = reduce_function(mapped_results)

    # 找出飞行次数最多的乘客及其飞行次数
    max_flights = max(reduced_result.values())
    max_flights_passenger = max(reduced_result, key=reduced_result.get)

    print(f'乘客中飞行次数最多的是: {max_flights_passenger}，飞行次数为: {max_flights}')
