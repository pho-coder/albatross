# albatross

the dispatch of plumber eggs on magpie, it is also a magpie eggs.

## Design

1. jobs and tasks life cycle only forward, no backward. So logic will be easy.
2. if jobs' or tasks' status is wrong, interrupt and clean all, and tell caller error.
3. retry. every part find itself exceptions, deal it(mostly retry), then tell status to caller.

## License

Copyright © 2015 FIXME

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
