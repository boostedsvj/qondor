"""# submit
htcondor(
    'cmsconnect_blacklist',
    ['RU', 'FNAL', 'IFCA', 'KIPT', 'T3_IT_Trieste', 'T2_TR_METU', 'T2_US_Vanderbilt', 'T2_PK_NCP']
    )

for i in range(0,10):
    for mz in [400]:
        submit(i=i, mz=mz, mdark=5., rinv=0.3)
"""# endsubmit

import qondor, seutils, os.path as osp
qondor.svj.use_prel_mgtarballs()

cmssw = qondor.svj.init_cmssw(
    'root://cmseos.fnal.gov//store/user/lpcsusyhad/SVJ2017/boosted/svjproduction-tarballs/CMSSW_10_2_21_latest_el7_gen_2018.tar.gz'
    )

physics = qondor.svj.Physics({
    'year' : 2018,
    'mz' : qondor.scope.mz,
    'mdark' : qondor.scope.mdark,
    'rinv' : qondor.scope.rinv,
    'mingenjetpt' : 375.,
    'max_events' : 1000,
    'part' : qondor.scope.i
    })

expected_outfile = cmssw.run_step('step0_GRIDPACK', 'step1_LHE-GEN', physics)

if not qondor.BATCHMODE: seutils.drymode()
seutils.cp(
    expected_outfile,
    'gsiftp://hepcms-gridftp.umd.edu//mnt/hadoop/cms/store/user/snabili/SIG/'
    'GENJets_{date}_mz{mz:.0f}_mdark{mdark:.0f}_rinv{rinv}/{i}.root'
    .format(
        date = qondor.get_submission_timestr(),
        i = qondor.scope.i,
        **physics
    ),
    env=qondor.BARE_ENV
        
    ) 

