import time

import ray
import requests

context = ray.init()
print(context.dashboard_url)
def request_data(n):
    start = time.time()
    print(f"requesting {n}")
    requests.get('https://5fe83520010a670017803dd6.mockapi.io/user')
    print(f"done {n} {time.time() - start}")

@ray.remote
def request_data_with_ray(n):
    print(f"requesting {n}")
    requests.get('https://5fe83520010a670017803dd6.mockapi.io/user')
    print(f"done {n}")


if __name__ == '__main__':
    print(context.dashboard_url)

    print('with ray')
    start = time.time()

    tasks = [request_data_with_ray.remote(i) for i in range(200)]
    print(ray.get(tasks))
    print(f"with ray: {time.time() - start}")
