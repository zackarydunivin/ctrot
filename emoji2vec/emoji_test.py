# -*- coding: utf-8 -*-
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import matplotlib

import numpy as np
# config the figure for bigger and higher resolution
#plt.rcParams["figure.figsize"] = [12.0, 8.0]
#plt.rcParams['figure.dpi'] = 300
data = np.random.randn(7, 2)
plt.scatter(data[:, 0], data[:, 1])
labels = u'😀 😃 😄 😁 😆 😅 😂 🤣 ☺️ 😊 😇'.split()
print(labels)
for label, x, y in zip(labels, data[:, 0], data[:, 1]):
    plt.annotate(
        label, # some of these contain Emojis
        xy=(x, y), xytext=(-20, 20),
        textcoords='offset points', ha='right', va='bottom',
        bbox=dict(boxstyle='round,pad=0.5', fc='yellow', alpha=0.5),
        arrowprops=dict(arrowstyle = '->', connectionstyle='arc3,rad=0'),
        fontname='Apple Color Emoji',
        fontsize=20)
#prop = fm.FontProperties(fname='/System/Library/Fonts/Apple Color Emoji.ttc')
#prop = fm.FontProperties(fname='/Users/zodcomp/Library/Fonts/seguiemj.ttf')
#plt.rcParams['font.family'] = prop.get_name()
#matplotlib.rcParams['font.family'] = prop.get_name()
plt.show()