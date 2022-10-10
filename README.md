# MIT6.824 - Distributed Systems 

## Labs

### MapReduce

```ad-info
title: General Suggestion
Model your system first according to requirements and then code it up.  Helps streamlining the coding process.
```

```ad-danger
title: Concurrency
* Not making `goroutines` for udpating status for each worker but having an *event-based* BS code (with the goroutines :) ). 

Q. $\text{How to identify seperate concurrency (ex:- CPU, I/O either network or disk) and leverage it for parallelism? What all techniques do exist?}$
```

