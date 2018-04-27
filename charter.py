import numpy as np
from matplotlib import pyplot as plt


def bar_chart(x,y):
    x_pos = np.arange(len(x))
    plt.bar(x_pos, y, align='center', alpha=0.5)
    plt.xticks(x_pos, x)
    plt.ylabel('utility score')
    plt.title('top '.join(str(len(x))).join(' utilities'))
    plt.show()
