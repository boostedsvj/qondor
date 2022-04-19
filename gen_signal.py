"""# submit
htcondor(
    'cmsconnect_blacklist',
    ['RU', 'FNAL', 'IFCA', 'KIPT', 'T3_IT_Trieste', 'T2_TR_METU', 'T2_US_Vanderbilt', 'T2_PK_NCP']
    )

for i in range(1,400):
    for mz in [250,]:
        submit(i=i, mz=mz, mdark=10, rinv=0.3)
"""# endsubmit

import qondor, seutils, os.path as osp

cmssw = qondor.svj.init_cmssw(
    'root://cmseos.fnal.gov//store/user/lpcsusyhad/SVJ2017/boosted/svjproduction-tarballs/CMSSW_10_6_29_latest_afce000_el7_miniaod_2018UL.tar.gz'
    )

physics = qondor.svj.Physics({
    'year' : 2018,
    'mz' : qondor.scope.mz,
    'mdark' : qondor.scope.mdark,
    'rinv' : qondor.scope.rinv,
    'boost': 375.,
    'max_events' : 100000,
    'part' : qondor.scope.i
    })

cmssw.download_madgraph_tarball(physics)
expected_outfile = cmssw.run_step('step0_GRIDPACK', 'step_LHE-GEN', physics)

if not qondor.BATCHMODE: seutils.drymode()
seutils.cp(
    expected_outfile,
    'root://cmseos.fnal.gov//store/user/lpcdarkqcd/MCSamples_UL_Spring2022/GEN/'
    'genjetpt375_{date}_mz{mz:.0f}_mdark{mdark:.0f}_rinv{rinv}/{i}.root'
    .format(
        date = qondor.get_submission_timestr(),
        i = qondor.scope.i,
        **physics
        )
    )
