#!/bin/bash
#1 获取输入参数个数，如果没有参数，直接退出
pcount=$#
if((pcount==0)); then
echo no args;
exit;
fi

#2 获取文件名称,注意引号是反引号
p1=$1
fname=`basename $p1`
echo fname=$fname

#3 并切换到文件所在目录，获取该目录绝对路径
pdir=`cd -P $(dirname $p1); pwd`
echo pdir=$pdir

#4 获取远程用户名
user='root'

#5 远程主机名由hadoop-node1,hadoop-node2,hadoop-node3.....组成，根据需要自己调整
for((host=2; host<6; host++)); do
        #echo $pdir/$fname $user@lan.node$host:$pdir
        echo --------------- hadoop-node$host ----------------
        echo ---------$pdir-$fname -------$user@lan.node$host:$pdir----------
        rsync -rvl $pdir/$fname $user@lan.node$host:$pdir
done
