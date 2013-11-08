#
# generate colors that look similar to the backgrounds used
# in bootstraps labels, by copying the same saturation and value
# that they use
# 

import colorsys

def generateColors(i):
  for j in range(i):
    color = colorsys.hsv_to_rgb(j * 1.0 / i,hsv[1],hsv[2])
    convertTo255 = lambda x : int(round(x * 255))
    print "$label-color-%d: rgb%s;" % (j,str(tuple(map(convertTo255, color))))
    
generateColors(16)