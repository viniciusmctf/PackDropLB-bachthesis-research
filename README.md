# PackDrop

A distributed package-oriented load balancer for *Charm++*.

## Add PackDrop to your Charm++ build

#### In charm/tmp/
Add PackDropLB.C, PackDropLB.h, PackDropLB.ci, and OrderedElement.h to your `charm/tmp` directory.
Add PackDropLB to your `COMMON_LBS` in your `Makefile_lb.sh`.
```
./Makefile_lb.sh
make depends -j
make charm++
```

#### In charm/src/ck-ldb/
Add PackDropLB.C, PackDropLB.h, PackDropLB.ci, and OrderedElement.h to your `charm/src/ck-ldb/` directory.
Add PackDropLB to your `COMMON_LBS` in your `Makefile_lb.sh`.
Rebuild your *Charm++*.
