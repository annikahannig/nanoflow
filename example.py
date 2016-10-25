from nanoflow import graph

import asyncio
import logging


@graph.op
def Half(x):
    return x * 0.5


@graph.op
def Quad(x):
    return x*x


@graph.op
def Sum(*args):
    return sum(args)


@graph.op
async def Factorialize(x):
    number = x
    f = 1
    for i in range(2, number+1):
        print("Fact(%s) - %s: Compute factorial(%s)..." % (x, number, i))
        await asyncio.sleep(1)
        f *= i
    return f



def make_model():
    x = graph.Placeholder(name="x")
    y = graph.Placeholder(name="y")

    x_fact = Factorialize(x)
    y_fact = Factorialize(y)

    xx = Quad(x)

    summed = Sum([x, xx, y, x_fact, y_fact])
    halfed = Half(summed)

    return halfed


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.set_debug(True)
    logging.basicConfig(level=logging.DEBUG)

    model = make_model()

    t1 = graph.run_async(model, feed_dict={'x': 2, 'y': 3})
    t2 = graph.run_async(model, feed_dict={'x': 4, 'y': 1})

    results = loop.run_until_complete(asyncio.gather(t1, t2))

    print("Results: {}".format(results))

    loop.close()


