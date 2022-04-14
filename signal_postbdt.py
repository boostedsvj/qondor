"""# submit
htcondor('request_memory', '4096MB')
import seutils, os.path as osp, itertools

print('Compiling list of rootfiles...')
# mz 250, 300, 350, 400, 450, 500, 550
sig_rootfiles = [ seutils.ls_wildcard(d + '/*.root') for d in [
    'root://cmseos.fnal.gov//store/user/lpcdarkqcd/MCSamples_Summer21/TreeMaker/genjetpt375_mz250_mdark10_rinv0.3',
    'root://cmseos.fnal.gov//store/user/lpcdarkqcd/MCSamples_Summer21/TreeMaker/genjetpt375_mz300_mdark10_rinv0.3',
    'root://cmseos.fnal.gov//store/user/lpcdarkqcd/MCSamples_Summer21/TreeMaker/genjetpt375_mz350_mdark10_rinv0.3',
    'root://cmseos.fnal.gov//store/user/lpcdarkqcd/MCSamples_Sept28/TreeMaker/genjetpt375_mz250_mdark10_rinv0.3',
    'root://cmseos.fnal.gov//store/user/lpcdarkqcd/MCSamples_Sept28/TreeMaker/genjetpt375_mz300_mdark10_rinv0.3',
    'root://cmseos.fnal.gov//store/user/lpcdarkqcd/MCSamples_Sept28/TreeMaker/genjetpt375_mz350_mdark10_rinv0.3',
    'root://cmseos.fnal.gov//store/user/lpcdarkqcd/MCSamples_Sept28/TreeMaker/genjetpt375_mz400_mdark10_rinv0.3',
    'root://cmseos.fnal.gov//store/user/lpcdarkqcd/MCSamples_Sept28/TreeMaker/genjetpt375_mz450_mdark10_rinv0.3',
    'root://cmseos.fnal.gov//store/user/lpcdarkqcd/MCSamples_Sept28/TreeMaker/genjetpt375_mz500_mdark10_rinv0.3',
    'root://cmseos.fnal.gov//store/user/lpcdarkqcd/MCSamples_Sept28/TreeMaker/genjetpt375_mz550_mdark10_rinv0.3',
    ]]
sig_rootfiles = list(itertools.chain.from_iterable(sig_rootfiles))

#print("signal rootfiles are: ", sig_rootfiles)
bdt_json = 'svjbdt_girthreweight_Jan06.json' #girth re-weight
#bdt_json = 'svjbdt_ptreweight_Jan06.json'
#bdt_json = 'svjbdt_massreweight_Jan06.json'
#bdt_json = 'svjbdt_Jan06.json'

def submit_chunk(chunk):
    submit(
        rootfiles=chunk,
        bdt_json=bdt_json,
        run_env='condapack:root://cmseos.fnal.gov//store/user/klijnsma/conda-svj-bdt.tar.gz',
        transfer_files=['combine_hists.py', 'dataset.py', bdt_json],
        )


for chunk in qondor.utils.chunkify(sig_rootfiles, chunksize=50):
    submit_chunk(chunk)
"""# endsubmit

import qondor, seutils
from combine_hists import dump_score_npz
import xgboost as xgb
import os.path as osp, os

model = xgb.XGBClassifier()
model.load_model(qondor.scope.bdt_json)

for rootfile in qondor.scope.rootfiles:
    try:
        seutils.cp(rootfile, 'in.root')
        dump_score_npz('in.root', model, 'out.npz')
        seutils.cp(
           'out.npz',
           #'gsiftp://hepcms-gridftp.umd.edu//mnt/hadoop/cms/store/user/snabili/NPZfiles_PostBDT/BDT_Sig_03142022_'
           'gsiftp://hepcms-gridftp.umd.edu//mnt/hadoop/cms/store/user/snabili/NPZfiles_PostBDT/BDT_Sig_04102022_masseq_met_'
           + '/'.join(rootfile.split('/')[-3:]).replace('.root', '.npz'),
           implementation='gfal', env=qondor.BARE_ENV),

    except Exception as e:
        print('Failed for rootfile ' + rootfile + ':')
        print(e)
        
    finally:
        if osp.isfile('out.npz'): os.remove('out.npz')
        if osp.isfile('in.root'): os.remove('in.root')
