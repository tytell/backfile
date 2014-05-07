'''
Created on Apr 18, 2012

@author: eric
'''

import time, sys, math


class ProgressNone(object):
    '''
    Base class for progress displays.
    Also used to skip progress output, since it doesn't display anything
    '''

    def __init__(self, **kw):
        return self
   
    def __enter__(self):
        return self
   
    def __exit__(self, type, value, traceback):
        pass
   
    def update(self, count, **kw):
        pass
 
class ProgressCLI(ProgressNone):
    '''
    Command line progress display.
    From http://code.activestate.com/recipes/473899-progress-meter/
    
    Here is a silly example of its usage:
    
    import progress
    import time
    import random
    
    total = 1000
    p = progress.ProgressMeter(total=total)
    
    while total > 0:
        cnt = random.randint(1, 25)
        p.update(cnt)
        total -= cnt
        time.sleep(random.random())
    
    
    Here is an example of its output:
    
    [------------------------->                                   ] 41%  821.2/sec    
    '''
    
    ESC = chr(27)
    iscursor = False
    
    def __init__(self, **kw):
        self.set(**kw)

        if self.total > 0:
            self.meter_ticks = int(kw.get('ticks', 60))
            self.meter_division = float(self.total) / self.meter_ticks
            self.meter_value = int(self.count / self.meter_division)
        self.abort = False
        self.last_update = None
        self.rate_history_idx = 0
        self.rate_history_len = 10
        self.rate_history = [None] * self.rate_history_len
        self.rate_current = 0.0
        self.last_refresh = 0
        self._cursor = False
        if (self.iscursor):
            self.reset_cursor()
        
    def set(self, **kw):
        # What time do we start tracking our progress from?
        self.timestamp = kw.get('timestamp', time.time())
        # What kind of unit are we tracking?
        self.unit = str(kw.get('unit', ''))
        # Number of units to process
        self.total = int(kw.get('total', 100))
        # Number of units already processed
        self.count = int(kw.get('count', 0))
        # Refresh rate in seconds
        self.rate_refresh = float(kw.get('rate_refresh', .5))
        # Number of ticks in meter
        if self.total > 0:
            self.meter_ticks = int(kw.get('ticks', 60))
            self.meter_division = float(self.total) / self.meter_ticks
            self.meter_value = int(self.count / self.meter_division)

    def __enter__(self):
        self.last_update = None
        self.rate_history_idx = 0
        self.rate_history_len = 10
        self.rate_history = [None] * self.rate_history_len
        self.rate_current = 0.0
        self.last_refresh = 0
        self._cursor = False
        if (self.iscursor):
            self.reset_cursor()
        return self
    
    def __exit__(self, type, value, traceback):
        if (self.count < self.total):
            self.abort = True
        self.refresh()
        
        
    def reset_cursor(self, first=False):
        if self._cursor:
            sys.stdout.write(self.ESC + '[u')
        self._cursor = True
        sys.stdout.write(self.ESC + '[s')

    def update(self, count, **kw):
        now = time.time()
        # Caclulate rate of progress
        rate = 0.0
        # Add count to Total
        self.count += count
        self.count = min(self.count, self.total)
        if self.last_update:
            delta = now - float(self.last_update)
            if delta:
                rate = count / delta
            else:
                rate = count
            self.rate_history[self.rate_history_idx] = rate
            self.rate_history_idx += 1
            self.rate_history_idx %= self.rate_history_len
            cnt = 0
            total = 0.0
            # Average rate history
            for rate in self.rate_history:
                if rate == None:
                    continue
                cnt += 1
                total += rate
            rate = total / cnt
        self.rate_current = rate
        self.last_update = now
        # Device Total by meter division
        value = int(self.count / self.meter_division)
        if value > self.meter_value:
            self.meter_value = value
            
        if self.last_refresh:
            if (now - self.last_refresh) > self.rate_refresh or \
                (self.count >= self.total):
                    self.refresh(**kw)
        else:
            self.refresh(**kw)

    def get_meter(self, **kw):
        bar = '-' * self.meter_value
        pad = ' ' * (self.meter_ticks - self.meter_value)
        perc = (float(self.count) / self.total) * 100
        return '[%s>%s] %d%%  %.1f/sec' % (bar, pad, perc, self.rate_current)

    def refresh(self, **kw):
        if self.total == 0:
            return
        
        # Clear line
        if self.iscursor:
            sys.stdout.write(self.ESC + '[2K')
            self.reset_cursor()
        sys.stdout.write(self.get_meter(**kw))
        if 'info' in kw:
            sys.stdout.write(' (' + kw['info'] + ')')
        # Are we finished?
        if self.abort or not self.iscursor or (self.count >= self.total):
            sys.stdout.write('\n')
        sys.stdout.flush()
        # Timestamp
        self.last_refresh = time.time()
