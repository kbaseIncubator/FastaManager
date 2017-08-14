# -*- coding: utf-8 -*-
#BEGIN_HEADER
from Workspace.WorkspaceClient import Workspace
from AssemblyUtil.AssemblyUtilClient import AssemblyUtil
from multiprocessing.queues import Queue
from multiprocessing.pool import ThreadPool
import time
import os
#END_HEADER


class FastaManager:
    '''
    Module Name:
    FastaManager

    Module Description:
    A KBase module: FastaManager
    '''

    ######## WARNING FOR GEVENT USERS ####### noqa
    # Since asynchronous IO can lead to methods - even the same method -
    # interrupting each other, you must be *very* careful when using global
    # state. A method could easily clobber the state set by another while
    # the latter method is running.
    ######################################### noqa
    VERSION = "0.0.1"
    GIT_URL = ""
    GIT_COMMIT_HASH = ""

    #BEGIN_CLASS_HEADER
    def get_genomes(self):
        bs=10000
        minid=0
        maxid=bs
        glist=[]
        for i in range(1,2):
            glist.extend(self.ws.list_objects({'workspaces':['ReferenceDataManager'],'type':'KBaseGenomes.Genome','minObjectID':minid,'maxObjectID':maxid}))
            minid+=bs
            maxid+=bs
            
        return glist

    def read_log(self):
        offsets = {}
        if not os.path.exists(self.logfile):
            return offsets
        with open(self.logfile, 'r') as lf:
            for line in lf:
                (ref, offset)=line.rstrip().split(':')
                offsets[ref] = offset

        return offsets
      
    def fix_and_merge(self, ref, fasta):
        tref=ref.replace('/','_')
        endl = 0
        if os.path.exists(self.merged):
          endl = os.stat(self.merged)[6]
        print "Fixing %s" % (fasta)
        with open(self.merged,'a') as o:
            with open(fasta,'r') as i:
               for line in i:
                   if line[0]=='>':
                      line = line.replace('>','>%s_'% (tref))
                   o.write(line)
        with open(self.logfile,'a') as l:
            l.write('%s:%d\n' % (ref, endl))
        print "Done with %s" % (fasta)
        os.remove(fasta)
        
    def get_fasta(self, ref, q):
        obj = self.ws.get_objects2({'objects':[{'ref':ref,'included':['assembly_ref']}]})
        aref = obj['data'][0]['data']['assembly_ref']
        res=self.au.get_assembly_as_fasta({'ref':aref})
        print aref,res
        q.put({'ref':ref, 'path':res['path']})
    #END_CLASS_HEADER

    # config contains contents of config file in a hash or None if it couldn't
    # be found
    def __init__(self, config):
        #BEGIN_CONSTRUCTOR
        self.workspaceURL = config['workspace-url']
        self.scratch = os.path.abspath(config['scratch'])
        self.callbackURL = os.environ.get('SDK_CALLBACK_URL')
        print "Callback=%s" % (self.callbackURL)
        self.ws = Workspace(self.workspaceURL)
        self.au = AssemblyUtil(self.callbackURL)
        self.logfile = '/kb/module/work/merged.log'
        self.merged = '/kb/module/work/merged.fa'
        #END_CONSTRUCTOR
        pass


    def build_fasta(self, ctx, params):
        """
        :param params: instance of unspecified object
        :returns: instance of type "BuildFastaOutput" -> structure: parameter
           "report_name" of String, parameter "report_ref" of String
        """
        # ctx is the context object
        # return variables are: output
        #BEGIN build_fasta
        offsets = self.read_log()
        genomes = self.get_genomes()
        q = Queue()
        ct = 0
        t = ThreadPool(processes=4)

        for genome in genomes:
           ref = '%s/%s/%s' % (genome[6],genome[0],genome[4])
           if ref in offsets:
               print "Skipping ref: %s" % (ref)
               continue
           t.apply_async(self.get_fasta, args=[ref, q])
           #apath = self.get_fasta(ref, q)
           ct+=1
        print ct
        while ct>0:
            res = q.get() 
            self.fix_and_merge(res['ref'], res['path'])
            ct-=1
        output = {}
        #END build_fasta

        # At some point might do deeper type checking...
        if not isinstance(output, dict):
            raise ValueError('Method build_fasta return value ' +
                             'output is not type dict as required.')
        # return the results
        return [output]
    def status(self, ctx):
        #BEGIN_STATUS
        returnVal = {'state': "OK",
                     'message': "",
                     'version': self.VERSION,
                     'git_url': self.GIT_URL,
                     'git_commit_hash': self.GIT_COMMIT_HASH}
        #END_STATUS
        return [returnVal]
