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
# Try to process more GEN files per job for low mz
# (because @ low mz, GEN files have few events)

# Gather all rootfiles
all_rootfiles = seutils.ls_wildcard('root://cmseos.fnal.gov//store/user/lpcdarkqcd/MCSamples_UL_Spring2022_Apr26/GEN/*/*.root')

# Function to get mass from a filename
get_mass = lambda rootfile: int(re.search(r'mz(\d+)', rootfile).group(1))

# Approximate number of GEN events per file per mass point
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
        # Aim for about 100 GEN events per job
        chunksize = max(int(100./nevents_per_rootfile[mz]), 1)
        yield from qondor.utils.chunkify(rootfiles, chunksize=chunksize)

for i, chunk in enumerate(yield_chunks()):
    submit(rootfiles=chunk)
    if i>0 and i % 100 == 0:
        submit_now()
        print('Sleeping for 30s to prevent scheduler overloading')
        sleep(30)
"""# endsubmit

import qondor, seutils, os.path as osp, os, argparse, re, glob

cmssw = qondor.svj.init_cmssw(
    'root://cmseos.fnal.gov//store/user/lpcdarkqcd/boosted/svjproductiontarballs/CMSSW_10_6_29_patch1_svjprod_el7_2018UL_b6852b4_May04_withHLTs.tar.gz'
    )
cmssw_for_hlt = qondor.svj.CMSSW(osp.join(cmssw.cmssw_path, '../HLT/CMSSW_10_2_16_UL/'))

# The physics parameters don't matter for the DIGI->MiniAOD chain
# Just set some random values so that SVJ/Production finds all the expected files
physics = qondor.svj.Physics({
    'year' : 2018,
    'mz' : 100,
    'mdark' : 10,
    'rinv' : .3,
    'max_events' : 100000,
    'part' : 1
    })

for rootfile in qondor.scope.rootfiles:
    qondor.logger.info('Processing ' + rootfile)
    stageout_location = rootfile.replace('/GEN/', '/MINIAOD/')

    if seutils.isfile(stageout_location):
        qondor.logger.info('Output file {} already exists; continuing'.format(stageout_location))
        continue

    output_rootfile = cmssw.run_chain(
        ['GEN', 'step_SIM', 'step_DIGI'],
        rootfile=rootfile,
        physics=physics
        )
    output_rootfile = cmssw_for_hlt.run_chain(
        ['step_DIGI', 'step_HLT'],
        rootfile=output_rootfile,
        physics=physics
        )
    output_rootfile = cmssw.run_chain(
        ['step_HLT', 'step_RECO', 'step_MINIAOD'],
        rootfile=output_rootfile,
        physics=physics
        )
    if not qondor.BATCHMODE: seutils.drymode()
    qondor.logger.warning('Copying ' + output_rootfile + ' -> ' + stageout_location)
    seutils.cp(output_rootfile, stageout_location)

    # Clean up any created rootfiles before moving on to the next one
    qondor.logger.warning('Cleaning up rootfiles')
    for c in [cmssw, cmssw_for_hlt]:
        for rootfile_to_rm in glob.glob(osp.join(c.cmssw_src, 'SVJ/Production/test/*.root')):
            qondor.logger.warning('Removing %s', rootfile_to_rm)
            os.remove(rootfile_to_rm)

    # # For UMD:
    # seutils.cp(
    #     output_rootfile,
    #     'gsiftp://hepcms-gridftp.umd.edu//mnt/hadoop/cms/store/user/snabili/BKG/'
    #     'miniAOD_{date}_mz{mz:.0f}_mdark{mdark:.0f}_rinv1p0/{i}.root'
    #     .format(
    #         date = qondor.get_submission_timestr(),
    #         i = qondor.scope.i,
    #         **physics
    #         ),
    #     env=qondor.BARE_ENV, implementation='gfal'
    #     )
