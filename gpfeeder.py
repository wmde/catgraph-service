# -*- coding: utf-8 -*-

import sys, os, os.path, pprint, traceback
import ConfigParser, optparse
import datetime, time
import pickle
import pprint
import threading
import traceback
import socket
import resource, struct
import code
import gc
import _mysql_exceptions

from gp.client import *
from gp.mysql import *
from gp.mediawiki import *
#from wikitools import wiki, api

class CommandStream(object):
    def __init__(self, f):
        if f is None:
            pass
        if type(file) in (str, unicode):
            f = file(f, "ra")
            self.close_file = True
        else:
            self.close_file = False
            
        if isinstance(f, socket.socket):
            f = f.makefile("ra")
            
        #NOTE: f could be a file or a fifo or a unix socket or a tcp socket...
            
        self.file = f
        self._closed = False
        
    def close(self):
        self._closed = True

        if self.close_file and self.file:
            self.file.close()
            self.file = None
            
        #FIXME: if file is None, we are using raw_input. close stdin to interrupt?!
        
    def __iter__(self):
        return self
        
    def next(self):
        while not self._closed:
            try:
                if self.file:
                    s = self.file.readline()
                    
                    if s is None or s == '': #EOF
                        break
                else:
                    s = raw_input("> ")

            except socket.timeout:
                #use timeouts to check self._closed
                continue
                
            except EOFError:
                break #EOF
            
            except KeyboardInterrupt:
                #self.feeder.warning("KeyboardInterrupt, terminating")
                s = "shutdown"
            
            s = s.strip()
            
            if s == '': # blank line
                continue
                
            if s.startswith('#') or s.startswith(';'): # comment line
                continue
                
            return s.split()
        
        self.close()
        raise StopIteration()
    
class Script(threading.Thread):
    def __init__(self, feeder, stream):
        super(Script, self).__init__()
        
        self.feeder = feeder
        
        if not isinstance(stream, CommandStream):
            stream = CommandStream(stream) # wrap handle or file path
            
        self._cmd_stream = stream
        
        self._stopped = False
        
    def stop(self):
        #XXX: this thread is probably currently blocking on I/O.
        #     how to interrupt it?
        
        self.feeder.trace("closing script loop")
        self._cmd_stream.close()
        self._stopped = True
        
    def run(self):
        self.feeder.trace("entering script loop")
        
        for cmd in self._cmd_stream:
            if self._stopped: 
                self._cmd_stream.close()
                break
            
            try:
                self.feeder.trace("running command %s" % cmd)
                self.run_command(cmd)
            except Exception as e:
                self.feeder.error("failed to run command %s: %s" % (cmd, e), e)
                
        self.feeder.trace("exiting script loop")
                
    def run_command(self, cmd):
        name = "cmd_%s" % cmd[0].replace('-', '_')
        
        args = []
        opt = {}
        
        if cmd[0] == "bye" or cmd[0] == "exit":
            self.stop()
            return
 
        if cmd[0] == "shutdown":
            self.stop()
        
        for c in cmd[1:]:
            (k, sep, v) = c.partition("=")
            
            if sep == '':
                args.append(c)
            else:
                opt[k]=v
        
        f = getattr(self.feeder, name) # let exception rip
        f(*args, **opt)
            

class Job(object):
    def __init__(self, feeder, wiki):
        self.feeder = feeder
        self.wiki = wiki
        
        self.done = False
        self.success = None
        self.followup = None
        self.time = None
        
        self.try_create = False
        
    def trace(self, msg):
        self.feeder.trace(msg)
        
    def log(self, msg):
        self.feeder.log(msg)
        
    def warning(self, msg):
        self.feeder.warning(msg)
        
    def error(self, msg, e = None):
        self.feeder.error(msg, e)
        
    def activate(self):
        # switch to the desired wiki
        self.feeder.connect( self.wiki, create = self.try_create ) 

    def reset(self):
        self.done = False
        
    def execute(self):
        if self.done:
            raise Exception("can't re-run a job! %s", self)
            
        self.start = datetime.datetime.now()
        
        try:
            self.activate()
            
            self.followup = self.run()
            
            self.success = True
            self.time = datetime.datetime.now() - self.start
            
            self.trace("finished %s after %s" % (self, self.time))
            return self.followup
        finally:
            self.done = True
        
    def run(self):
        raise Error("must implement run()")
        
    def __str__(self):
        return "%s ( wiki = %s )" % (self.__class__.__name__, self.wiki)
        
class LoadChunkJob(Job):
    def __init__(self, feeder, wiki, namespaces, from_page, offset, limit = None, update_followup = None):
        super(LoadChunkJob, self).__init__(feeder, wiki)
        
        assert type(offset) == int
        
        self.update_followup = update_followup
        
        self.namespaces = namespaces
        self.chunk_size = limit
        self.from_page = from_page
        self.offset = offset
        
        if not self.namespaces:
            self.cl_types = None
        else:
            self.cl_types = set()

            if NS_CATEGORY in self.namespaces:
                self.cl_types.add( "subcat" )
            
            if NS_FILE in self.namespaces:
                self.cl_types.add( "file" )
                
            if len(self.cl_types) < len(self.namespaces): # if there's more in self.namespaces...
                self.cl_types.add( "page" )
        
    def __str__(self):
        return "%s ( wiki = %s, namespaces = %s, from_page = %d, offset = %d )" % (self.__class__.__name__, self.wiki, self.namespaces, self.from_page, self.offset)
        
    def run(self):
        wiki_config = feeder.get_wiki_config(self.wiki)
        
        if ( 'load-query-limit' in wiki_config
            and wiki_config['load-query-limit'] 
            and int(wiki_config['load-query-limit']) > 0 ):
                
            limit = int(wiki_config['load-query-limit']) 
        else:
            if self.offset:
                limit = 1000000000
            else:
                limit = None
            
        self.log( 'loading cat structure from page %d, offset %d ' % (self.from_page, self.offset) )
        
        sql = "SELECT P.page_id as parent, C.page_id as child, C.page_title as name "
        sql += " FROM " + self.feeder.gp.wiki_table( "categorylinks" ) 
        sql += " JOIN " + self.feeder.gp.wiki_table( "page" ) + " AS P "
        sql += " ON P.page_title = cl_to AND P.page_namespace = %d " % NS_CATEGORY
        sql += " JOIN " + self.feeder.gp.wiki_table( "page" ) + " AS C "
        sql += " ON C.page_id = cl_from "
        
        where = []
        
        if self.namespaces:
            where.append( " C.page_namespace IN %s " % self.feeder.gp.as_list( self.namespaces ) )
            where.append( " cl_type IN %s " % self.feeder.gp.as_list( self.cl_types ) )
        
        if self.from_page:
            where.append( " C.page_id >= %d " % self.from_page )
            
        if where:
            sql += " WHERE ( %s ) " % " ) AND ( ".join ( where ) 
            
        if limit: # if there is no limit, we don't need to sort!
            sql += " ORDER BY C.page_id, P.page_id "
        
        if limit:
            sql += " LIMIT %d " % limit 

        if self.offset:
            sql += " OFFSET %d " % self.offset
            
        self.log( 'executing query for category arcs: %s' % sql )
        src = self.feeder.gp.make_source( MySQLSelect( sql ), comment = self.feeder.slow_query_comment, big = True )
        
        self.trace( 'fetching category arcs' )

        cont = ChunkTracer()
        cont.offset = self.offset
            
        if ( 'load-chunk-size' in wiki_config 
            and wiki_config['load-chunk-size'] 
            and int( wiki_config['load-chunk-size'] ) > 0 ):
                
            chunk_size = int(wiki_config['load-chunk-size'])
        else:
            chunk_size = None

        self.feeder.add_arcs( src, cont, chunk_size = chunk_size )
        
        src.close()
        
        self.log( 'loaded %d arcs' % cont.count ) 

        if not limit or cont.count == 0: # done loading
            self.feeder.set_meta(status = "loaded")

            return self.update_followup
        
        assert cont.id, "Don't know continuation ID even though query returned results"
        
        if cont.count < limit:
            self.feeder.set_meta(status = "loaded", load_pos = cont.id, load_offset = cont.offset)

            return self.update_followup
        else:
            self.feeder.set_meta( status = "loading", load_pos = cont.id, load_offset = cont.offset )

            return LoadChunkJob( self.feeder, self.wiki, self.namespaces, from_page = cont.id, offset = cont.offset, update_followup = self.update_followup )

class StartLoadJob(Job):
    def __init__(self, feeder, wiki, namespaces, start_polling = False):
        super(StartLoadJob, self).__init__(feeder, wiki)
        
        self.namespaces = namespaces
        
        self.start_polling = start_polling
        self.try_create = True

    def __str__(self):
        return "%s ( wiki = %s, start_polling = %s )" % (self.__class__.__name__, self.wiki, self.start_polling)

    def run(self):
        wiki_config = feeder.get_wiki_config(self.wiki)
        self.log("loading category structure of %s" % self.wiki)

        up_current = self.feeder.get_latest_category_timestamp()
        dl_current = self.feeder.get_latest_log_timestamp()
        
        if not up_current:
            up_current = "00000000000000"
        
        if not dl_current:
            dl_current = "00000000000000"
        
        if self.start_polling:
            # get job that will launch polling, starting with the current state (before loading the structure)
            
            update_job = []
            update_job.append( UpdateModifiedJob(self.feeder, self.wiki, self.namespaces, since = up_current, keep_polling = self.start_polling) )
            update_job.append( UpdateDeletedJob(self.feeder, self.wiki, self.namespaces, since = dl_current, keep_polling = self.start_polling) )
        else:
            update_job = None

        feeder_state = self.feeder.get_meta()
        
        #reset state for incremental processing
        feeder_state["mods_offset"] = 0
        feeder_state["dels_offset"] = 0
        feeder_state["mods_until"] = up_current
        feeder_state["dels_until"] = dl_current
        feeder_state["mods_state"] = "init"
        feeder_state["dels_state"] = "init"
        feeder_state["status"] = "loading"
        feeder_state["load_offset"] = 0
        feeder_state["load_pos"] = 0

        if not self.namespaces:
            feeder_state["namespaces"] = "*"
        else:
            feeder_state["namespaces"] = "+".join( [ str(n) for n in self.namespaces ] )
            
        if self.namespaces and NS_CATEGORY in self.namespaces and len(self.namespaces) == 1:
            feeder_state["graph_type"] = "no-leafs"
        else:
            feeder_state["graph_type"] = "with-leafs"
            
        self.feeder.set_meta(**feeder_state)

        g = wiki_config['gp-graph']
        
        #TODO: create with temp name, rename when import done
        #TODO: we want graphserv to support rename-graph and replace-graph
        #      note: existing connections should switch to new graph seemlessly. 
        #      note: rename blocks until there's no active command on the graph

        created = self.feeder.gp.try_create_graph(g)
        if created: 
            self.log( "created graph %s" % g)
        else:
            self.log( 'clearing graph %s' % g )        
            self.feeder.gp.clear()

        next_job = LoadChunkJob(self.feeder, self.wiki, self.namespaces, 0, 0, update_followup = update_job)
        return next_job

class UpdateModifiedJob(Job):
    def __init__(self, feeder, wiki, namespaces, since = None, offset = 0, keep_polling = False):
        super(UpdateModifiedJob, self).__init__(feeder, wiki)
        
        assert type(offset) == int
        
        self.keep_polling = keep_polling
        self.namespaces = namespaces
        
        self.since = since
        self.offset = offset
        
        self.state_name = "mods"
        
    def __str__(self):
        return "%s ( wiki = %s, state_name = %s, since = %s, offset = %d )" % (self.__class__.__name__, self.wiki, self.state_name, self.since, self.offset)
        
    def make_followup(self, since = None, offset = 0, keep_polling = None):
        assert type(offset) == int

        if keep_polling is None:
            keep_polling = self.keep_polling
            
        return self.__class__(self.feeder, self.wiki, namespaces = self.namespaces, since = since, offset = offset, keep_polling = keep_polling) 
        
    def run(self):
        wiki_config = feeder.get_wiki_config(self.wiki)
        limit = int(wiki_config['update-max-cats'])

        feeder_state = self.feeder.get_meta()
        
        if not "status" in feeder_state or not feeder_state["status"]:
            raise gpException( "not yet loaded (no feeder status)" )
        elif feeder_state["status"] == "loading":
            raise gpException( "loadeding not yet complete (feeder: %s)" % feeder_state["status"] )
        
        #FIXME: verify that our namespace set is the same as the original in feeder_state
        #XXX: Or just use the namespaces set in feeder_state??
        
        if self.since is None:
            offset = feeder_state.get(self.state_name+'_offset')
            since  = feeder_state.get(self.state_name+'_until')
        else:
            since = self.since
            offset = self.offset
            
        if not since:
            self.error("can't apply an update without a baseline. If the graph contains no meta-vars describing the update state, you must provide the --update-since option.")
            return False
        
        if offset is None:
            offset = 0

        if self.keep_polling:
            feeder_state['status'] = "polling"
        else:
            feeder_state['status'] = "updating"

        seen = set()
        items = 0
        max_timestamp = since
        
        self.trace( 'updating %s since %s, offset %s ' % (self.state_name, since, offset) )

        cats = self.get_modified_pages(since, offset, limit)
        
        if not cats:
            #update done
            
            if self.keep_polling:
                # keep polling for updates

                feeder_state[self.state_name+'_state'] = "up_to_date"
                self.feeder.set_meta(**feeder_state)

                # rely on stored state to pass since/offset to the next update job
                return self.make_followup(keep_polling = True)
            else:
                self.log( 'update complete' )
                self.feeder.set_meta(status = "updated")
                
                return None

        pg = {} # reusable dict for page entries

        for pg_row in cats:
            # assign fields of reusable page dict
            # ...the meaning of the column is arcane knowledge... 
            pg['id'] =        pg_row[0]
            pg['title'] =     pg_row[1]
            pg['namespace'] = pg_row[2]
            pg['timestamp'] = pg_row[3]
            pg['i'] =         pg_row[4]
            
            page_id = pg['id']
            
            if pg['timestamp'] > max_timestamp:
                offset = 0
                max_timestamp = pg['timestamp']

            offset += 1
            
            if page_id in seen:
                continue

            seen.add(page_id)
            items += 1
            
            self.log( 'updating %s: category %s ' % (self.state_name, pg['title']) )
            self.update_categorization( pg ) 

        self.log( 'updated %d %s since %s until %s' % (items, self.state_name, since, max_timestamp) )
        
        # store update state
        feeder_state[self.state_name+'_offset'] = offset
        feeder_state[self.state_name+'_until'] = max_timestamp
        feeder_state[self.state_name+'_state'] = "catching_up"
        
        self.feeder.set_meta(**feeder_state)
        
        # not done yet, keep going. 
        # rely on stored state to pass since/offset to the next update job
        
        followup = self.make_followup()
        return followup
                
    def get_modified_pages(self, since, offset, limit):
        mods = self.feeder.get_touched_categories( since = since, offset = offset, limit = limit )
        return mods
        
    def update_categorization(self, pg ):
        n = self.feeder.update_category_children( pg, namespaces = self.namespaces ) 
        return n
        
class UpdateDeletedJob(UpdateModifiedJob):
    def __init__(self, feeder, wiki, namespaces, since = None, offset = 0, keep_polling = False):
        super(UpdateDeletedJob, self).__init__(feeder, wiki, namespaces = namespaces, since = since, offset = offset, keep_polling = keep_polling)
        self.state_name = "dels"
        
    def get_modified_pages(self, since, offset, limit):
        mods = self.feeder.get_deleted_pages( since = since, namespaces = self.namespaces, offset = offset, limit = limit )
        return mods
        
    def update_categorization(self, pg ):
        n = self.feeder.remove_node( pg )        
        return n
        
PROC_STAT_FIELDS = (
    "pid",
    "comm",
    "state",
    "ppid",
    "pgrp",
    "session",
    "tty_nr",
    "tpgid",
    "flags",
    "minflt",
    "cminflt",
    "majflt",
    "cmajflt",
    "utime",
    "stime",
    "cutime",
    "cstime",
    "priority",
    "nice",
    "dummy",
    "itrealvalue",
    "starttime",
    "vsize",
    "rss",
)

PROC_PSINFO_STRUCT = (
    # Perl:   iiii iiii iiIi iiiS Sa8a8a8 Z16Z80ii IIaa3 iiia
    # Python: iiii iiii iiIi iiiH H8s8s8s 16s80sii IIc3s iiic
    
    ("flag", "i", 4), 
    ("nlwp", "i", 4), 
    ("pid", "i", 4), 
    ("ppid", "i", 4),
     
    ("pgid", "i", 4), 
    ("sid", "i", 4),
    ("uid", "i", 4),
    ("euid", "i", 4),
    
    ("gid", "i", 4), 
    ("egid", "i", 4), 
    ("addr", "i", 4), 
    ("vsize", "i", 4),
    
	("rss", "i", 4),
    ("pad1", "i", 4),
    ("ttydev", "i", 4),
    ("pctcpu", "H", 2),
    
    ("pctmem", "H", 2),
	("start", "8s", 8),
    ("time", "8s", 8),
    ("ctime", "8s", 8),
    
    ("fname", "16s", 16),
    ("psargs", "80s", 80),
	("wstat", "i", 4),
    ("argc", "i", 4),
    
    ("argv", "i", 4),
    ("envp", "i", 4),
    ("dmodel", "c", 1),
	("taskid", "3s", 3),
    
    ("projid", "i", 4),
    ("nzomb", "i", 4),
    ("filler_1", "i", 4),
    ("filler_2", "c", 1),
)

PROC_PSINFO_FIELDS = [ r[0] for r in PROC_PSINFO_STRUCT ]
PROC_PSINFO_PATTERN = " ".join( [ r[1] for r in PROC_PSINFO_STRUCT ] )
PROC_PSINFO_SIZE = reduce( lambda acc, c: acc+c, [ r[2] for r in PROC_PSINFO_STRUCT ] )

def proc_stat( pid = None ):
    if not pid:
        pid = os.getpid()
    
    try:
        f = file("/proc/%d/stat" % pid)
        s = f.read()
        f.close()
        
        r = s.split(" ")
        
        return dict(zip(PROC_STAT_FIELDS, r))
        
    except IOError:
        pass #not supported, /proc/.../stat is a Linuxism!
        
    try:
        f = file("/proc/%d/psinfo" % pid)
        s = f.read()
        f.close()
        
        pattern = PROC_PSINFO_PATTERN
        
        if len(s) > PROC_PSINFO_SIZE: #ignore extra data
            pattern += " %ds" % ( len(s) - PROC_PSINFO_SIZE, )
        
        r = struct.unpack(pattern, s)
        
        stats = dict(zip(PROC_PSINFO_FIELDS, r))
        
        stats['vsize'] *= 1024 #solaris provides KB here, apparently
        
        return stats
        
    except IOError:
        pass #not supported, /proc/.../psinfo is a Solarisism!
        
    return False

class ChunkTracer(object):
    """callback for tracing and muning items while loading the category structure"""
    
    def __init__(self):
        self.id = None
        self.name = None
        
        self.offset = 0
        self.count = 0
    
    def __call__( self, *args ):
        row = args[0]
        id = row[1]
        
        if self.id is None or self.id != id:
            self.id = id
            self.name = row[2]
            self.offset = 1
        else:
            self.offset += 1
            
        self.count += 1
        
        return row[0:2]
        
    def __str__(self):
        if not self.id:
            return "none"
        else:
            return "%d rows, up to %s, offset %d" % (self.count, self.name, self.offset)
        
class Feeder(object):
    def __init__(self, options):
        self.wiki_states = {}
        self.wiki_config = None
        self.current_wiki = None
        
        self.gp = None
        self.slow_query_comment = None
        
        self.config = None #XXX: automatically load options here?
        self.options = options
        
        self.jobs = [] # job queue
        self.job_lock = threading.Lock()
        
        self.verbose = options.verbose
        self.debug = options.debug

        self._stopped = False
        self._frozen = False
        
        self.terminate_when_empty = False

        # profilers, etc
        self.heapy = None
        self.dowser = None
        
        if options.heapy:
            self.log("preparing heapy memory profiler.")
            self.get_heapy()
            
        if options.dowser:
            self.log("preparing dowser memory profiler.")

            import dowser
            self.dowser = dowser.Root()

        if options.cherrypy:
            self.log("preparing cherrypy browser")
            import cherrypy
            
            if self.dowser:
                self.trace("mounting dowser into cherrypy")
                cherrypy.tree.mount(self.dowser)
            elif self.heapy:
                self.trace("mounting heapy into cherrypy")
                cherrypy.tree.mount(self.heapy)
                
            cherrypy.config.update({
                'environment': 'embedded',
                'server.socket_port': int(options.cherrypy_port)
            })
            
            self.log("starting cherrypy browser on port %s" % options.cherrypy_port)
            cherrypy.server.quickstart()
            cherrypy.engine.start()
            
    def log_prefix(self):
        return time.strftime("%Y-%m-%d %H:%M:%S")
        
    def trace(self, msg):
        if self.verbose:
            print self.log_prefix(), "    ", msg
        
    def log(self, msg):
        print self.log_prefix(), msg
        
    def warning(self, msg):
        print self.log_prefix(), "WARNING:", msg
        
    def error(self, msg, e = None):
        print self.log_prefix(), "ERROR:", msg
        
        if e and self.verbose:
            traceback.print_exc()
        
    def get_meta(self, use_cache = True):
        state = None
        
        if self.gp.supportsProtocolVersion(4): # meta-vars supported since prot version 4
            meta = self.gp.capture_list_meta_map()
            state = {}
            
            for (k, v) in meta.items():
                if k.startswith( "gpfeeder_" ):
                    k = k[9:]
                    state[k] = v
            
        elif 'state-file' in self.wiki_config:
            if not use_cache or not self.current_wiki not in self.wiki_states:
                p = self.wiki_config['state-file']
                
                if os.path.exists( p ):
                    try:
                        f = file( p, 'r' )
                        self.wiki_states = pickle.load( f )
                        f.close()
                    except IOError as (errno, strerror):
                        self.warning( "Coudn't load state: I/O error({0}): {1}".format(errno, strerror) )
                    except EOFError as e:
                        self.warning( "Coudn't load state: %s" % sys.exc_info()[0] )
            
        if state is None:
            if self.current_wiki in self.wiki_states:
                state = self.wiki_states[self.current_wiki]
            else:
                state = {}
                
        self.trace("get_meta: %s => %s" % (self.current_wiki, state))
        return state
        
    def set_meta(self, **state_map):
        state_map["timestamp"] = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
        
        self.trace("set_meta: %s => %s" % (self.current_wiki, state_map))
        
        if self.gp.supportsProtocolVersion(4): # meta-vars supported since prot version 4
            for (k, v) in state_map.items():
                if v is None or v == False or v == "":
                    self.gp.try_remove_meta("gpfeeder_" + k)
                else:
                    self.gp.set_meta("gpfeeder_" + k, v)
    
        else:
            if self.current_wiki in self.wiki_states:
                self.wiki_states[self.current_wiki].update( state_map )
            else:
                self.wiki_states[self.current_wiki] = state_map
            
            if 'state-file' in self.wiki_config:
                p = self.wiki_config['state-file']
                
                f = file( p, 'w' )
                pickle.dump( self.wiki_states, f )
                f.close()
                         
    def get_latest_rcid( self, before = None ):
        sql = "SELECT MAX(rc_id) "
        sql += " FROM " + self.gp.wiki_table( "recentchanges" )
        
        if before:
            sql += " WHERE rc_timestamp < %s " % self.gp.quote_string(before)

        self.trace("get_latest_rcid(%s)" % before)
        rcid = self.gp.mysql_query_value( sql )
        return rcid
        
    def get_latest_rc_timestamp( self ):
        sql = "SELECT MAX(rc_timestamp) "
        sql += " FROM " + self.gp.wiki_table( "recentchanges" )
        
        self.trace("get_latest_rc_timestamp")
        t = self.gp.mysql_query_value( sql )
        return t
        
    def get_latest_log_timestamp( self ):
        sql = "SELECT MAX(log_timestamp) "
        sql += " FROM " + self.gp.wiki_table( "logging" )
        
        self.trace("get_latest_log_timestamp")
        t = self.gp.mysql_query_value( sql )
        return t
        
    def get_latest_category_timestamp( self ):
        sql = "SELECT MAX(page_touched) "
        sql += " FROM " + self.gp.wiki_table( "page" )
        sql += " WHERE page_namespace = %d " % NS_CATEGORY
        
        self.trace("get_latest_category_timestamp")
        t = self.gp.mysql_query_value( sql, comment = self.slow_query_comment )
        return t
        
    def get_deleted_pages( self, since, namespaces, limit = None, offset = None ):
        if offset is None:
            offset = 0
        
        # deleted category pages
        # note: we need to join against archive to get the page id, since log_page_id is 0 for deletions.
        
        self.gp.mysql_query( "SET @counter = 0" ); #use counter instead of native offset
        
        sql  = "SELECT ar_page_id as id, log_title as title, log_namespace as namespace, max(log_timestamp) as log_timestamp, @counter:=@counter+1 as i "
        sql += " FROM " + self.gp.wiki_table( "logging" )
        sql += " JOIN " + self.gp.wiki_table( "archive" )
        sql += "   ON ar_namespace = log_namespace AND ar_title = log_title "
        
        sql += " WHERE log_type = 'delete' AND log_action = 'delete' "
        sql += "   AND log_timestamp >= %s " % self.gp.quote_string( since )
        
        if namespaces:
            sql += "   AND log_namespace IN %s " % self.gp.as_list( namespaces )
        
        sql += " GROUP BY ar_page_id " #XXX: this could make the limit expensive, if since is far in the past.
        sql += " HAVING log_timestamp > %s " % self.gp.quote_string( since )
        sql += "   OR i > %d " % offset
        sql += " ORDER BY log_timestamp, log_id " # NOTE: must be monotonously increasing over time
        
        if limit:
            sql += " LIMIT %d " % limit

        #XXX: This doesn't actually work! entries in the deletion log have log_page=0 always!
        #     There's apparently no way to get the id of the deleted page. 
        src = self.gp.make_source( MySQLSelect( sql ) ) 
        dels = src.drain()
        src.close()
        
        return dels
        
    def get_touched_categories( self, since, limit = None, offset = None ):
        if offset is None:
            offset = 0
        
        # touched category pages

        self.gp.mysql_query( "SET @counter = 0" ); #use counter instead of native offset
        
        sql  = "SELECT page_id as id, page_title as title, page_namespace as namespace, page_touched as touched, @counter:=@counter+1 as i "
        sql += " FROM " + self.gp.wiki_table( "page" )
        sql += " WHERE page_namespace = %d " % NS_CATEGORY
        sql += "   AND page_touched >= %s " % self.gp.quote_string( since )
        sql += " HAVING page_touched > %s " % self.gp.quote_string( since )
        sql += "   OR i > %d " % offset
        sql += " ORDER BY page_touched, page_latest " # NOTE: hopefully be monotonously increasing over time
        
        if limit:
            sql += " LIMIT %d " % limit
        
        src = self.gp.make_source( MySQLSelect( sql ) )
        mods = src.drain()
        src.close()
        
        return mods
        
    def update_categorization( self, pg ):
        sql = "SELECT P.page_id as parent "
        sql += " FROM " + self.gp.wiki_table( "categorylinks" ) 
        sql += " JOIN " + self.gp.wiki_table( "page" ) + " AS P "
        sql += " ON P.page_title = cl_to AND P.page_namespace = %d " % NS_CATEGORY
        sql += " WHERE cl_from = %d " % pg['id']
        
        src = self.gp.make_source( MySQLSelect( sql ) )
        n = src.result.rowcount # XXX: this is kind of hackish and should perhaps be encapsulated in in Source object

        self.gp.replace_predecessors( pg['id'], src )
        src.close()
        
        return n
        
    def update_category_children( self, pg, namespaces = None ):
        sql = "SELECT cl_from "
        sql += " FROM " + self.gp.wiki_table( "categorylinks" ) 
        
        if namespaces:
            sql += " JOIN " + self.gp.wiki_table( "page" ) + " AS P "
            sql += " ON P.page_id = cl_from "
        
        sql += " WHERE cl_to = %s " % self.gp.quote_string( pg['title'] )
        
        if namespaces:
            sql += " AND P.page_namespace IN %s " % self.gp.as_list( namespaces )
        
        src = self.gp.make_source( MySQLSelect( sql ) )
        n = src.result.rowcount # XXX: this is kind of hackish and should perhaps be encapsulated in in Source object

        self.gp.replace_successors( pg['id'], src )
        src.close()
        
        return n

    def remove_node( self, pg ):
        #TODO: we *really*  want graphcore to support remove-node!
        
        if pg['id'] <=0:
            return
        
        pre = self.gp.capture_list_predecessors( pg['id'] )
        succ = self.gp.capture_list_successors( pg['id'] )
        
        arcs = [ (p[0], pg['id']) for p in pre ] + [ (pg['id'], s[0]) for s in succ ]
        
        self.gp.remove_arcs( arcs )

    def add_arcs( self, src, tracer = None, chunk_size = None ):
        if ( chunk_size ):
            #XXX: the caller may already know the number of rows in src.
            #     we could shorten out if we know that it was less than chunk_size!
        
            while True: # loop though chunks
                chunk_src = LimitedSource( src, chunk_size )
                self.gp.add_arcs( chunk_src, tracer )
                
                self.trace( "added a chunk of %d arcs " % chunk_src.index )
                
                if not chunk_src.limit_reached():
                    break # if the limit wasn't reached, there's no more data in src.
        else:
            # if there's no chunk limit
            self.gp.add_arcs( src, tracer )

    def drop_wiki_graph( self, wiki ):
        wiki_config = self.get_wiki_config(wiki)
        pre = self.gp.drop_graph( wiki_config['gp_graph'] )

    def disconnect( self ):
        if self.gp:
            self.gp.close() #FIXME: graphserve bug: connection stays open
            self.gp = None
        
    def connect( self, wiki, create = False ):
        self.trace("connect to wiki %s" % wiki)
        
        wiki_config = self.get_wiki_config(wiki)
        
        old_wiki_config = self.wiki_config
        self.wiki_config = None
        self.current_wiki = None
        
        if ( old_wiki_config is not None and self.gp 
            and old_wiki_config['gp-host'] == wiki_config['gp-host']
            and old_wiki_config['gp-port'] == wiki_config['gp-port'] ):

            ok = False

            try:
                if old_wiki_config['gp-graph'] != wiki_config['gp-graph']:
                    self.trace("switching to graph %s" % wiki_config['gp-graph'])
                    self.gp.use_graph(wiki_config['gp-graph']) 
                else:
                    self.gp.ping() # check alive #TODO: make this optional
                    
                ok = True
            except gpException as e:
                self.warning( "%s" % e )

            if ok:
                self.trace("re-using gp connection to %s" % wiki_config['gp-host'])
            else:
                self.trace("closing broken gp connection")
                self.gp.close() #FIXME: graphserve bug: connection stays open
                self.gp = None
                
        elif self.gp:
                self.trace("closing old gp connection")
                self.gp.close()
                self.gp = None
        
        if not self.gp:
            self.log("creating fresh gp connection to %s" % wiki_config['gp-host'])
            
            self.gp = MediaWikiGlue.new_client_connection(None, wiki_config['gp-host'], wiki_config.getint('gp-port') )
            self.gp.setDebug( self.debug )
            self.gp.connect()
            
            if wiki_config['gp-auth'] and wiki_config['gp-credentials']:
                self.gp.authorize(wiki_config['gp-auth'], wiki_config['gp-credentials'])
                
            if wiki_config['gp-graph']:
                g = wiki_config['gp-graph']
                
                if create: 
                    created = self.gp.try_create_graph(g)
                    if created: self.log("created graph %s" % g)
                    #else: self.log("could not create graph %s" % g)
                    
                self.gp.use_graph(wiki_config['gp-graph'])

            #TODO: set debug if desired
        
        if ( old_wiki_config is not None and self.gp.connection 
            and old_wiki_config['mysql-host'] == wiki_config['mysql-host']
            and old_wiki_config['mysql-port'] == wiki_config['mysql-port'] ):

            ok = False

            try:
                self.trace("switching to database %s on %s" % (wiki_config['mysql-database'], wiki_config['mysql-host']))
                self.gp.mysql_select_db( wiki_config['mysql-database'] ) 
                ok = True
            except _mysql_exceptions.OperationalError as e:
                self.warning( "MySQL error: %s" % e )
            except IOError as (errno, strerror):
                self.warning( "I/O error({0}): {1}".format(errno, strerror) )
                
            if ok:
                self.trace("re-using db connection to %s" % wiki_config['mysql-host'])
            else:
                self.trace("closing broken db connection")
                self.gp.connection.close() #TODO: encapsulate!
                self.gp.connection = None
                
        elif self.gp.connection:
                self.trace("closing old db connection")
                self.gp.connection.close() #TODO: encapsulate!
                self.gp.connection = None
                
        if not self.gp.connection:
            h = wiki_config['mysql-host']
            if 'mysql-port' in wiki_config and wiki_config['mysql-port']:
                h += ":" + wiki_config['mysql-port']
                
            self.log("creating fresh db connection to %s" % h)
                
            self.gp.mysql_connect( server = wiki_config['mysql-host'], 
                                    port = int(wiki_config['mysql-port']), 
                                    db = wiki_config['mysql-database'], 
                                    username = wiki_config['mysql-user'], 
                                    password = wiki_config['mysql-password'] )
                                    
            self.gp.mysql_autocommit(True) #NOTE: need that for read too, so we see changes at all
            
        self.gp.table_prefix = wiki_config['mysql-prefix']

        if 'mysql-temp-db' in wiki_config:
            self.gp.temp_table_db = wiki_config['mysql-temp-db']
        
        if 'slow-query-comment' in wiki_config:
            self.slow_query_comment = wiki_config['slow-query-comment']
        
        self.wiki_config = wiki_config
        self.current_wiki = wiki
        
    def add_job(self, job):
        with self.job_lock:
            self.trace("add_job: %s" % job)
            
            try: #try whether job is actually a sequence
                self.jobs.extend( job )
            except: #not a sequence, just one job
                self.jobs.append( job )

            self.trace("jobs now: %s" % self.jobs)

    def remove_jobs(self, wiki):
        self.trace("remove_jobs: %s" % (wiki,) )
        
        c = 0
        with self.job_lock:
            i = 0
            while i < len(self.jobs):
                j = self.jobs[i]
                
                if j.wiki == wiki:
                    self.trace("remove_jobs: removing %s" % j)
                    del self.jobs[i]
                    c += 1
                else:
                    i += 1
                    
        self.trace("jobs now: %s" % self.jobs)
        return c
        
    def run_next_job(self):
        if not self.jobs:
            return None
            
        with self.job_lock:
            job = self.jobs[0]
            del self.jobs[0]
        
            self.trace("popped job %s " % job)
            self.trace("jobs now: %s" % self.jobs)
        
        done = False

        try:
            followup = job.execute()
            done = True
            
            if followup:
                self.add_job( followup )
                
        except IOError as (errno, strerror):
            self.error( "I/O error({0}): {1}\n".format(errno, strerror), (errno, strerror) )
        except gpException as e:
            self.error( "GraphServ error: %s\n" % e, e )
        except ConfigParser.NoSectionError as e:
            self.error( "Configuration error: %s\n" % e, e )
        except ConfigParser.NoOptionError as e:
            self.error( "Configuration error: %s\n" % e, e )
        except _mysql_exceptions.OperationalError as e:
            self.error( "MySQL error: %s\n" % e, e )
            
        if not done:
            job.reset()
            
            with self.job_lock:
                self.trace("queueing job after error: %s " % job)
                self.jobs.append(job)
                
                self.trace("jobs now: %s" % self.jobs)
            
        return job
        
    def stop(self):
        self.trace("stopping feeder's job loop")
        self._stopped = True
        
        with self.job_lock:
            self.jobs[:] = [] #clear
            
        self.disconnect()
        
    def run_jobs( self, delay = None ):
        try:
            while ( not self._stopped 
                and ( self.jobs or not self.terminate_when_empty ) ):
                    
                if self._frozen:
                    self.log("job processing loop frozen")
                    
                    while self._frozen and not self._stopped:
                        time.sleep(1) # polling is not elegant, but simple and reliable
                        
                    if self._stopped:
                        break

                    self.log("job processing loop unfrozen")
                    
                self.run_next_job()
                
                if delay:
                    time.sleep(delay)
                    
        except KeyboardInterrupt:
            self.warning("KeyboardInterrupt, terminating")
            self.cmd_shutdown() 
            
            if threading.current_thread().name != "MainThread": #XXX: is there a better way?
                threading.interrupt_main() 
        
    def schedule_load(self, wikis, start_polling = False):
        for wiki in wikis:
            wiki_config = self.get_wiki_config(wiki)
            namespaces = self.get_namespaces( wiki_config ) 

            job = StartLoadJob(self, wiki, namespaces, start_polling = start_polling)
            self.add_job( job )

    def schedule_update(self, wikis, since = None, keep_polling = False):
        for wiki in wikis:
            wiki_config = self.get_wiki_config(wiki)
            namespaces = self.get_namespaces( wiki_config ) 

            job = UpdateModifiedJob(self, wiki, namespaces = namespaces, since = since, keep_polling = keep_polling)
            self.add_job( job )

            job = UpdateDeletedJob(self, wiki, namespaces = namespaces, since = since, keep_polling = keep_polling)
            self.add_job( job )
        
    def get_namespaces(self, wiki_config):
        if not 'namespaces' in wiki_config:
            return None
            
        nn = wiki_config['namespaces']
        
        if type(nn) in (str, unicode):
            nn = nn.split(",")
            
        namespaces = set()
        
        for n in nn:
            namespaces.add( int(n) )
            #TODO: catch ValueError, try namespace names!
        
        return namespaces
        
        
    def get_wiki_config(self, w):
        if w == self.current_wiki and self.wiki_config is not None:
            return self.wiki_config
            
        wiki_config = ConfigDict(self.config, w)
        
        #inject derivative defaults
        if 'gp-graph' not in wiki_config or wiki_config['gp-graph'] is None:
            wiki_config['gp-graph'] = wiki_config['mysql-database']
        
        if 'gp-credentials' not in wiki_config or wiki_config['gp-credentials'] is None:
            cred= wiki_config['mysql-user'] + ":" + wiki_config['mysql-password']
            wiki_config['gp-credentials'] = cred
            
        return wiki_config
                    
            
    def cmd_help(self, *wikis, **opt):
        print_script_help() 
        
    def cmd_verbose(self, *wikis, **opt):
        self.verbose = True
        self.debug = False
        
        if self.gp:
            self.gp.setDebug(False)
        
    def cmd_debug(self, *wikis, **opt):
        self.verbose = True
        self.debug = True
        
        if self.gp:
            self.gp.setDebug(True)
            
    def cmd_hush(self, *wikis, **opt):
        self.verbose = False
        self.debug = False
        
        if self.gp:
            self.gp.setDebug(False)
        
    def cmd_freeze(self, *wikis, **opt):
        self._frozen = True
        
    def cmd_unfreeze(self, *wikis, **opt):
        self._frozen = False
        
    def cmd_jobs(self, *wikis, **opt):
        if not self.jobs:
            print "no jobs"
            return
        
        for j in self.jobs:
            print j
        
    def cmd_shutdown(self, *wikis, **opt):
        self.stop() 
        
    def cmd_die(self, *wikis, **opt):
        self.warning("dieing")
        os._exit(11) #FIXME: leaves tty dirty. but sys.exit doesn't work!
        
    def cmd_reload_config(self, *wikis, **opt):
        self.load_config() 
        
    def cmd_update(self, *wikis, **opt):
        opt['keep_polling'] = False
        self.schedule_update(self, wikis, **opt)
        
    def cmd_start(self, *wikis, **opt):
        opt['keep_polling'] = True
        self.schedule_update(self, wikis, **opt)
        
    def cmd_stop(self, *wikis, **opt):
        for w in wikis:
            self.remove_jobs(w)
        
    def cmd_drop(self, *wikis, **opt):
        for w in wikis:
            self.remove_jobs(w)
            self.drop_wiki_graph(w) 
        
    def cmd_launch(self, *wikis, **opt):
        self.cmd_stop(wikis, opt)

        self.load_config()

        opt['start_polling'] = True
        self.schedule_load(wikis, **opt)
            
    def cmd_load(self, *wikis, **opt):
        self.cmd_stop(wikis, opt)

        self.load_config()
        
        opt['start_polling'] = False
        self.schedule_load(wikis, **opt)
                    
    def cmd_list_jobs(self, *wikis, **opt):
        for j in self.jobs:
            print j
        
    def cmd_gc(self, *wikis, **opt):
        gc.collect(2) # run full garbage collection of all generations
        
    def get_heapy(self):
        if not self.heapy:
            try:
                import guppy
                self.heapy = guppy.hpy()
                
                self.log("loaded guppy library.")
            except ImportError as ex:
                self.warning("guppy library not installed: %s" % ex)

        return self.heapy
        
    def cmd_heapy(self, *wikis, **opt):
        hpy = self.get_heapy()
        
        print hpy.heap()
        print ""
        print hpy.iso(self)

    def cmd_heapy_browser(self, *wikis, **opt):
        hpy = self.get_heapy()

        print "launching profile browser"
        hpy.pb()

    def cmd_prstat(self, *wikis, **opt):
        usage = resource.getrusage(resource.RUSAGE_SELF)
        pgsize = resource.getpagesize()
        
        print "utime: %d sec" % (usage.ru_utime,)
        print "stime: %d sec" % (usage.ru_stime,)
        if usage.ru_maxrss: print "peak physical memory (maxrss): %d KB" % (usage.ru_maxrss * pgsize / 1024,)
        if usage.ru_idrss: print "current physical memory (idrss): %d KB" % (usage.ru_idrss * pgsize / 1024,)
        if usage.ru_ixrss: print "current shared memory (ixrss): %d KB" % (usage.ru_ixrss * pgsize / 1024,)
        
        st = proc_stat()
        if st:
            if "rss" in st: print "current physical memory (rss): %d KB" % (int(st["rss"]), )
            if "vsize" in st: print "current virtual memory (vsize): %d KB" % (int(st["vsize"]) / 1024, )
        
    def cmd_heapy_baseline(self, *wikis, **opt):
        if not self.heapy:
            raise Exception("heapy not loaded")
            
        self.heapy.setref()
        print "new baseline set for heapy"
        
    def cmd_py(self, *wikis, **opt):
        if not self.options.interactive:
            print "python shell is only allowed in interactive mode (use --interactive)"
            return
        
        print "launching python shell. use EOF (Ctrl-D) to return."
        code.interact( banner="++++++++++++++++ Python Shell ++++++++++++++++", 
                        local = { "feeder" : self, 
                                    "gpfeeder": globals(), 
                                    "options": self.options,
                                    "jobs": self.jobs, } )
        print "python shell terminated, welcome back."

        
    def start_job_worker(self, delay = 1, daemon = False):
        worker = threading.Thread( name="Job Worker", target = self.run_jobs, kwargs = { "delay": poll_delay } )
        worker.daemon = daemon
        worker.start()
        return worker
        
    def run_script(self, script):
        loop = Script(self, script)
        loop.run()
        
    def start_script(self, script, daemon = False):
        loop = Script(self, script)
        loop.daemon = daemon
        
        loop.start()
        return loop
        
    def load_config(self):
        # find config file........
        bindir=  os.path.dirname( os.path.realpath( sys.argv[0] ) )
        
        if self.options.config_file:
            cfg = self.options.config_file #take it from --config
        else:
            cfg = bindir + "/gpfeeder.ini" #installation root
            
        config_defaults = {}

        # read .my.cnf........
        dbcnf_path = os.path.expanduser( "~/.my.cnf" )
        
        config_defaults['mysql-host'] = 'localhost'
        config_defaults['mysql-port'] = "3306"
        config_defaults['mysql-database'] = 'wiki'
        config_defaults['mysql-user'] = 'gpfeeder'
        config_defaults['mysql-password'] = 'gpfeeder'
        
        if os.path.exists(dbcnf_path):
            self.log( "reading database config from %s" % dbcnf_path )
            dbcnf = ConfigParser.SafeConfigParser(allow_no_value=True)
            dbcnf.add_section( 'client' )

            try:
                if dbcnf.read( dbcnf_path ):
                    for n in ('host', 'port', 'database', 'user', 'password'):
                        if dbcnf.has_option("Client", n):
                            config_defaults['mysql-' + n] = dbcnf.get("Client", n)
                        elif dbcnf.has_option("DEFAULT", n):
                            config_defaults['mysql-' + n] = dbcnf.get("DEFAULT", n)
            except ConfigParser.Error:
                self.warning( "failed to read mysql client config from %s." % dbcnf_path  )
                    
        
        # define more config defaults........
        config_defaults['mysql-prefix'] = '' 
        
        config_defaults['gp-host'] = 'localhost' 
        config_defaults['gp-port'] = PORT
        config_defaults['gp-auth'] = 'password' 
        #config_defaults['gp-graph'] = ''
        config_defaults['gp-credentials'] = None
        config_defaults['gp-graph'] = None
        config_defaults['load-chunk-size'] = '32000' 
        config_defaults['update-max-cats'] = '100' 
        config_defaults['load-query-limit'] = '' 
        
        config_defaults['state-file'] = bindir + '/gpfeeder-state.p' 
        #config_defaults['state-table'] = 'gpfeeder_state' 
        
        config_defaults['poll-delay'] = "1"

        # load config........
        config = ConfigParser.SafeConfigParser(defaults = config_defaults)

        if os.path.exists(cfg):
            self.log( "reading config file %s" % cfg )

            if not config.read( cfg ):
                self.error( "failed to read config from %s" % cfg )
                return False

        self.config = config
        return True
        
                    
class ConfigDict(object):
  def __init__(self, config, section):
      self.config = config
      self.section = section

  def __getitem__(self, k):
      return self.config.get(self.section, k)

  def __delitem__(self, k):
      return self.config.remove_option(self.section, k)

  def __setitem__(self, k, v):
      return self.config.set(self.section, k, str(v))

  def __contains__(self, k):
      return self.config.has_option(self.section, k)

  def __iter__(self):
      return self.iterkeys()

  def iterkeys(self):
      for key, value in self.items():
          yield key

  def items(self):
      return self.config.items(self.section)

  def initialize(self, k, v):
      if not k in self:
          self[k] = str(v)

  def __len__(self):
      return len( self.items() )
      
  def get(self, k):
      if k in self:
          return self[k]
      else:
          return None
      
  def getint(self, k):
      v = self.get(k)
      if v is None: return None
      
      return int(v)


##################################################################################

def print_script_help():
    print "gpfeeder can process very simple scripts, in the form of lists of "
    print "commands. Commands are processed on per line of input. Available "
    print "commands are: "
    print " "
    print "    help               show this help text"
    print "    load <wiki>...     loads structure of the given wikis"
    print "    launch <wiki>...   loads wikis and starts polling for updates"
    print "    update <wiki>...   updates the structure of the given wikis"
    print "    start <wiki>...    starts polling for changes on the wikis"
    print "    stop <wiki>...     stops polling for the given wikis"
    print "    drop <wiki>...     stops polling and removes structures from graphserv"
    print "    reload-config      reloads the configuration file"
    print " "
    print "    freeze             freeze job queue processing"
    print "    unfreeze           unfreeze job queue processing"
    print "    exit               terminate script (for interactive mode)"
    print "    shutdown           shuts down the gpfeeder"
    print " "
    print "    verbose            enables verbose output."
    print "    debug              enables debug (and verbose) output."
    print "    hush               disabled debug and verbose output."
    print " "
    print "    jobs               list current job queue."
    print "    prstat             output process statistics (system memory, etc)."
    print "    heapy              uses guppy's heapy to show heap summary."
    print "    gc                 run full garbage collection."
    print "    py                 starts interactive python shell."
    print "                       NOTE: works in interactive mode only."
    
    
def print_config_help():
    bindir=  os.path.dirname( os.path.realpath( sys.argv[0] ) )
    
    print "gpfeeder requires a configuration file to operate. Per default, the "
    print "configuration is loaded from pgfeeder.ini in the directory where "
    print "gpfeeder.py is located, that is, from: "
    print "    " + bindir + "/gpfeeder.ini"
    print "A different configuration file may be specified using the --config"
    print "option."
    print " "
    print "The configuration file use the INI-file syntax (as specified by python's "
    print "ConfigParser class). Note that no settings may occurr outside sections."
    print "Each section in the config file provides settings for a specific wiki."
    print "A special section called DEFAULT may be used to provide default settings"
    print "that apply to all wikis."
    print " "
    print "The following settings controll the connection to the MySQL database: "
    print "    mysql-host      the host to connect to. Default is 'localhost'"
    print "    mysql-port      the port to connect to. Default is 3306"
    print "    mysql-user      the user for authenticatin to MySQL. Default is 'gpfeeder'"
    print "    mysql-password  the password for authenticatin to MySQL. Default: 'gpfeeder'"
    print "    mysql-database  the database containing the wiki. Default is 'wiki'"
    print "    mysql-prefix    the table prefix used by the wiki. Default is none ('')"
    print "    mysql-temp-db   database to use for temp tables (default: use the wiki's database)"
    print " "
    print "If not given in the config file, the settings for mysql-host, mysql-port, mysql-user "
    print "mysql-password and mysql-database are taken from the respective settings in the"
    print "standard MySQL client configuration file (.my.cnf) in the user's home directory."
    print " "
    print "The following settings controll the connection to the graphserve instance: "
    print "    gp-host         the host to connect to. Default is the mysql-host setting."
    print "    gp-port         the port to connect to. Default is 6666"
    print "    gp-auth         the authentication method. Default is 'password'"
    print "    gp-credentials  the authentication credentials. For password auth, use"
    print "                    user:password syntax. The default is derived from the "
    print "                    mysql-user and mysql-password settings."
    print "    gp-graph        the graph name for the wiki. Default is the "
    print "                    mysql-database setting."
    print " "
    print "The following settings controll the operation of gpfeeder: "
    print "    load-chunk-size  the maximum number of arcs to push into graphserv in a"
    print "                     single command when initially loading a category structure."
    print "                     The default is 32000 arcs."
    print "    load-query-limit the maximum number of arcs to fetch from the database in"
    print "                     in one go while loading the category structure."
    print "    update-max-cats  the maximum number of categories to process for a given wiki"
    print "                     during an update pass. The default is 100 categories."
    print "                     Note that since there are two types of updates (modifications "
    print "                     and deletions), and this limit is applied to each kind. "
    print "    state-file       the file to store the update state in. With the help of this"
    print "                     file, gpfeeder can start the next update pass exactly where"
    print "                     the last one (or the original import) left off. Per default, "
    print "                     gpfeeder-state.p in the same directory as gpfeeder.py is used."
    print "                     Note that this file will only be used if it is not possible to"
    print "                     maintain the state in graphserv itself (graphserv protocol v4)."
    print "    poll-delay       the number of seconds to wait between polls. This is intended "
    print "                     to avoid busy waiting. The default value is 1 second. This "
    print "                     setting may be overridden using the --poll-delay option."
    print "    slow-query-comment  a comment to be injected into SQL queries that are expected "
    print "                        to be slow. "
    
    
def get_options():
    bindir=  os.path.dirname( os.path.realpath( sys.argv[0] ) )

    option_parser = optparse.OptionParser()
    option_parser.set_usage("gpfeeder.py [options] [wiki...]")
    option_parser.add_option("--config", dest="config_file", 
                                help="config file", metavar="FILE")
    option_parser.add_option("--config-help", dest="config_help", action="store_true", 
                                help="show configuration file help and exit" )
    option_parser.add_option("--script-help", dest="script_help", action="store_true", 
                                help="show script syntax help and exit" )
    option_parser.add_option("--verbose", dest="verbose", action="store_true",
                                help="enable verbose output (fairly noisy)")
    option_parser.add_option("--debug", dest="debug", action="store_true",
                                help="enable debug output (extremly noisy)")
    option_parser.add_option("--load", dest="load", action="store_true",
                                help="load category structure")
    option_parser.add_option("--update", dest="update", action="store_true",
                                help="update category structure once and exit")
    option_parser.add_option("--update-since", dest="update_since", 
                                help="start updates from date/time RC_TIMESTAMP (single wiki only)", metavar="RC_TIMESTAMP")
    option_parser.add_option("--poll", dest="poll", action="store_true",
                                help="poll for updates periodically, do not terminate")
    option_parser.add_option("--poll-delay", dest="poll_delay", default = 1, type = int,
                                help="sleep for SEC seconds between polls", metavar="SEC")
    option_parser.add_option("--all", dest="all", action="store_true",
                                help="process all wikis defined in the config file")
    option_parser.add_option("--interactive", dest="interactive", action="store_true",
                                help="accept interactive commands from stdin")
    option_parser.add_option("--script", dest="script", 
                                help="read commands from script (or fifo)")
                                
    option_parser.add_option("--shell", dest="shell", action="store_true",
                                help="starts a python shell after all scripts are complete.")
    option_parser.add_option("--heapy", dest="heapy", action="store_true",
                                help="use guppy's heapy library for memory profiling.")
    option_parser.add_option("--dowser", dest="dowser", action="store_true",
                                help="use dowser library for memory profiling (experimental).")
    option_parser.add_option("--cherrypy", dest="cherrypy", action="store_true",
                                help="use cherrypy to provide web interface for debugging. Currently only works with --dowser.")
    option_parser.add_option("--cherrypy-port", dest="cherrypy_port", default=8008, metavar="P",
                                help="port to use for cherrypy web server (default: 8008).")
                                
    (options, args) = option_parser.parse_args()
    
    options.script_on_stdin = ( not os.isatty(sys.stdin.fileno()) )
    
    if options.script_on_stdin:
        options.interactive = False
    
    if options.config_help:
        print_config_help()
        sys.exit(0)
    
    if options.script_help:
        print_script_help()
        sys.exit(0)
    
    if ( not options.load and not options.update and not options.poll 
        and not options.script and not options.interactive and not options.script_on_stdin ):
        sys.stderr.write( "Nothing to do. Use at least one of --load, --update, --poll, --script, or --interactive\n"  )
        sys.exit(1)

    if options.all and args:
        sys.stderr.write( "Conflicting arguments: do not list wikis if --all is provided.\n"  )
        sys.exit(1)

    if options.shell and options.interactive:
        sys.stderr.write( "Conflicting arguments: do not use --shell and --interactive together.\n"  )
        sys.exit(1)

    if (options.all or options.load) and options.update_since:
        sys.stderr.write( "Conflicting options: do not use --update-since together with --load or --all.\n" )
        sys.exit(1)

    if ( not options.all and not args and not options.script_on_stdin 
        and not options.script and not options.interactive ):
            
        args = ( "DEFAULT", )
        
    options.verbose = ( options.verbose or options.debug )
    
    return (args, options)
    
if __name__ == '__main__':
    (wikis, options) = get_options()
    
    if options.poll_delay:
        poll_delay = options.poll_delay
    else:
        poll_delay = config.getint("DEFAULT", 'poll-delay')
        
    gc.enable()
        
    try:
        feeder = Feeder(options)
        
        if not feeder.load_config(): #XXX: do that implicitly somewhere?
            sys.exit(1)

        if options.all:
            wikis = feeder.config.sections() # go through all explicitly defined wikis
            
            if not wikis:
                feeder.error( "No sections found in %s\n" % cfg )
                feeder.error( "If --all is given, the config file must define a section for each wiki to process." )
                sys.exit(1)
                
        if options.script:
            static_script = os.path.isfile(options.script) # reading from a fifo?
        else:
            static_script = True

        if options.load:
            feeder.schedule_load(wikis, start_polling = options.poll)
        else:
            feeder.schedule_update(wikis, since = options.update_since, keep_polling = options.poll)
            
        worker = feeder.start_job_worker( delay = poll_delay, daemon = options.interactive )
                    
        if options.script:

            if static_script:
                feeder.log("reading script from %s" % options.script)
            else:
                if options.script_on_stdin or options.interactive:
                    feeder.error("Conflicting options: can not read script from fifo and also handle stdin.")
                    sys.exit(1)
                    
                feeder.log("reading commands from pipe at %s" % options.script)

                feeder.run_script(options.script, daemon = not static_script)

        if options.script_on_stdin or options.interactive:
            if options.script_on_stdin:
                feeder.log("reading script from stdin")
                loop = feeder.run_script(sys.stdin)
            else:
                #interactive
                feeder.log("++++++++++++++++++ reading interactive commands from stdin ++++++++++++++++++")
                
                import readline
                loop = feeder.run_script(None) #use raw_input, which uses readline
                feeder.log("input closed, stopping")
                feeder.stop()

        if options.shell:
            feeder.log("launching python shell.")
            code.interact( banner="++++++++++++++++ Python Shell ++++++++++++++++", 
                            local = { "feeder" : feeder, 
                                        "worker": worker, 
                                        "gpfeeder": globals(), 
                                        "wikis": wikis, 
                                        "options": options } )

        feeder.terminate_when_empty = True

        while worker.isAlive():
            #XXX: ugly hack: use laszy polling spin lock, becahuse Thread.join ignores KeyboardInterrupt
            time.sleep(0.200)

    except IOError as (errno, strerror):
        sys.stderr.write( "FAILED: I/O error({0}): {1}\n".format(errno, strerror) )
    except gpProtocolException as e:
        sys.stderr.write( "FAILED: GraphServ error: %s\n" % e )
    except gpProcessorException as e:
        sys.stderr.write( "FAILED: GraphServ error: %s\n" % e )
    except ConfigParser.NoSectionError as e:
        sys.stderr.write( "FAILED: Configuration error: %s\n" % e )
    except ConfigParser.NoOptionError as e:
        sys.stderr.write( "FAILED: Configuration error: %s\n" % e )
    except _mysql_exceptions.OperationalError as e:
        sys.stderr.write( "FAILED: MySQL error: %s\n" % e )
    except (KeyboardInterrupt, SystemExit):
        sys.stderr.write( "INTERRUPTED: doing cleanup and shutting down\n" )
        feeder.stop()
