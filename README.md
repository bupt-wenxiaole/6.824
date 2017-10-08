# 6.824
This project was forked from MIT 6.824, All developers are from Applied computing lab, Insititue of computing, CAS
before run, you should get alluxio-go and Json-iterator in this module like this:
<pre><code>git clone https://github.com/Alluxio/alluxio-go.git
go get github.com/json-iterator/go</code></pre>

1. mapreduce implementation of golang 
2. mapreduce depends on alluxio
3. mapreduce with distributed system
4. fix the "can't handle 10GB upper files" bug
5. simplify mapF and reduceF, replace the self-make functions with raw functions (like make blank in the text i.e.)
6. use Json-iterator to accelerate encode&decode

