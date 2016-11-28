#!/usr/bin/python
# -*- coding: utf-8 -*-
import urllib, re, sys

posts = []
dates = []

def latin1_to_ascii (unicrap):
    xlate={0xc0:'A', 0xc1:'A', 0xc2:'A', 0xc3:'A', 0xc4:'A', 0xc5:'A',
           0xc6:'Ae', 0xc7:'C',
           0xc8:'E', 0xc9:'E', 0xca:'E', 0xcb:'E',
           0xcc:'I', 0xcd:'I', 0xce:'I', 0xcf:'I',
           0xd0:'Th', 0xd1:'N',
           0xd2:'O', 0xd3:'O', 0xd4:'O', 0xd5:'O', 0xd6:'O', 0xd8:'O',
           0xd9:'U', 0xda:'U', 0xdb:'U', 0xdc:'U',
           0xdd:'Y', 0xde:'th', 0xdf:'ss',
           0xe0:'a', 0xe1:'a', 0xe2:'a', 0xe3:'a', 0xe4:'a', 0xe5:'a',
           0xe6:'ae', 0xe7:'c',
           0xe8:'e', 0xe9:'e', 0xea:'e', 0xeb:'e',
           0xec:'i', 0xed:'i', 0xee:'i', 0xef:'i',
           0xf0:'th', 0xf1:'n',
           0xf2:'o', 0xf3:'o', 0xf4:'o', 0xf5:'o', 0xf6:'o', 0xf8:'o',
           0xf9:'u', 0xfa:'u', 0xfb:'u', 0xfc:'u',
           0xfd:'y', 0xfe:'th', 0xff:'y',
           0xa1:'!', 0xa2:'{cent}', 0xa3:'{pound}', 0xa4:'{currency}',
           0xa5:'{yen}', 0xa6:'|', 0xa7:'{section}', 0xa8:'{umlaut}',
           0xa9:'{C}', 0xaa:'{^a}', 0xab:'<<', 0xac:'{not}',
           0xad:'-', 0xae:'{R}', 0xaf:'_', 0xb0:'{degrees}',
           0xb1:'{+/-}', 0xb2:'{^2}', 0xb3:'{^3}', 0xb4:"'",
           0xb5:'{micro}', 0xb6:'{paragraph}', 0xb7:'*', 0xb8:'{cedilla}',
           0xb9:'{^1}', 0xba:'{^o}', 0xbb:'>>',
           0xbc:'{1/4}', 0xbd:'{1/2}', 0xbe:'{3/4}', 0xbf:'?',
           0xd7:'*', 0xf7:'/'
           }
    
    r = ''
    for i in unicrap:
        if xlate.has_key(ord(i)):
            r += xlate[ord(i)]
        elif ord(i) >= 0x80:
            pass
        else:
            r += i
    return r

deb = 4900
fin = 39543

if (len(sys.argv) > 1):
    deb = int(sys.argv[1])
if (len(sys.argv) > 2):
    fin = int(sys.argv[2])
if (len(sys.argv) > 3):
    filename = sys.argv[3]
filename = "trackmania.forum."+str(deb)+"."+str(fin-1)+".txt"
print "Début=", deb, "Fin=", fin
print "Fichier", filename

for n in range(deb,fin):
    url = "http://www.trackmania.com/fr/forum/viewtopic.php?t="+str(n)
    print url
    f = open(filename,"a+")
    content = urllib.urlopen(url).read()
    content = re.sub('<br />', '', content)
    content = re.sub('<span class="postbody"></span>', '', content)
    content = re.sub('<td class="quote">[^<]*', '', content)
    
    for post in re.finditer('<span class="postbody">([^<]*)', content):
        posts.append(' '.join(re.sub("[^a-zA-Z ]"," ",latin1_to_ascii(post.group(1)).lower()).split()))
        
    for date in re.finditer('([0-9]{1,2} [a-zA-Z]{3} [0-9]{4} [0-9]{1,2}:[0-9]{2})<span class="gen"', content):
        dates.append(date.group(1))

    for i in range(0,len(dates)):
        f.write(dates[i])
        f.write(":")
        f.write(posts[i])
        f.write("\n")
    f.close()

