"""# submit
import seutils
import re
from time import sleep

# Not needed on the LPC, only on cmsconnect:
htcondor(
    'cmsconnect_blacklist',
    ['RU', 'FNAL', 'IFCA', 'KIPT', 'T3_IT_Trieste', 'T2_TR_METU', 'T2_US_Vanderbilt', 'T2_PK_NCP']
    )

htcondor('request_memory', '4000 MB')

# ___________________________________
# Try to process more MINIAOD files per job for low mz
# (because @ low mz, MINIAOD files have few events)

# Gather all rootfiles
all_rootfiles = seutils.ls_wildcard('root://cmseos.fnal.gov//store/user/lpcdarkqcd/MCSamples_UL_Spring2022_Apr26/MINIAOD/*/*.root')

# Function to get mass from a filename
get_mass = lambda rootfile: int(re.search(r'mz(\d+)', rootfile).group(1))

# Approximate number of events per file per mass point
nevents_per_rootfile = {
    250 : 7,
    300 : 12,
    350 : 17,
    400 : 20,
    450 : 23,
    500 : 29,
    550 : 40,
    600 : 58,
    }

# Function to yield the actual chunks of rootfiles
def yield_chunks():
    rootfiles_per_mz = {}
    for rootfile in all_rootfiles:
        mz = get_mass(rootfile)
        rootfiles_per_mz.setdefault(mz, [])
        rootfiles_per_mz[mz].append(rootfile)
    for mz, rootfiles in sorted(rootfiles_per_mz.items()):
        # Aim for about 200 GEN events per job
        chunksize = int(200./nevents_per_rootfile[mz])
        yield from qondor.utils.chunkify(rootfiles, chunksize)
# ___________________________________

for i, chunk in enumerate(yield_chunks()):
    submit(rootfiles=chunk)
    if i>0 and i % 100 == 0:
        print('Sleeping for 30s to prevent scheduler overloading')
        sleep(30)
"""# endsubmit

import qondor, seutils, os.path as osp, os

cmssw = qondor.init_cmssw(
    'root://cmseos.fnal.gov//store/user/lpcdarkqcd/boosted/svjproductiontarballs/'
    'CMSSW_10_6_29_patch1_treemaker_el7_2018_f9c563c_Jun03.tar.gz'
    )

scenario = {
    # '2018' : 'Autumn18sig',
    '2018' : 'Summer20UL18sig',
    '2017' : 'Fall17sig',
    '2016' : 'Summer16v3sig',
    }

expected_outfile = osp.join(cmssw.cmssw_src, 'TreeMaker/Production/test/outfile_RA2AnalysisTree.root')


for rootfile in qondor.scope.rootfiles:

    if osp.isfile(expected_outfile):
        qondor.logger.warning('Removing preexisting ' + expected_outfile)
        os.remove(expected_outfile)

    cmssw.run_commands([
        'cd TreeMaker/Production/test',
        '_CONDOR_CHIRP_CONFIG="" cmsRun runMakeTreeFromMiniAOD_cfg.py'
        ' numevents=-1'
        ' outfile=outfile'
        ' scenario={}'
        ' lostlepton=0'
        ' doZinv=0'
        ' systematics=0'
        ' deepAK8=0'
        ' deepDoubleB=0'
        ' doPDFs=0'
        ' nestedVectors=False'
        ' debugjets=0'
        ' splitLevel=99'
        ' boostedsemivisible=1'
        ' dataset={}'
        .format(scenario['2018'], rootfile)
        ])

    # Stageout
    if not qondor.BATCHMODE: seutils.drymode() # Don't copy in local running mode
    seutils.cp(
        expected_outfile,
        rootfile.replace('MINIAOD', 'TREEMAKER')
        )
