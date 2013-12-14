  $ osdmaptool --create-from-conf om -c $TESTDIR/ceph.conf.withracks > /dev/null
  osdmaptool: osdmap file 'om'
  $ osdmaptool --test-map-pg 0.0 om
  osdmaptool: osdmap file 'om'
   parsed '0.0' -> 0.0
  0.0 raw ([], p-1) up ([], p-1) acting ([], p-1)
