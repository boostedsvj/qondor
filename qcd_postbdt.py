"""# submit
htcondor('request_memory', '4096MB')
import seutils, os.path as osp, itertools

print('Compiling list of rootfiles...')
# qcd 2018 
bkg_rootfiles = [ seutils.ls_wildcard(d + '/*.root') for d in [
    'root://cmseos.fnal.gov//store/user/lpcdarkqcd/boosted/umd/BKG/bkg_Mar25_year2018/Autumn18.QCD_Pt_300to470_TuneCP5_13TeV_pythia8',
    'root://cmseos.fnal.gov//store/user/lpcdarkqcd/boosted/umd/BKG/bkg_Apr26_year2018/Autumn18.QCD_Pt_300to470_TuneCP5_13TeV_pythia8',
    'root://cmseos.fnal.gov//store/user/lpcdarkqcd/boosted/umd/BKG/bkg_May04_year2018/Autumn18.QCD_Pt_300to470_TuneCP5_13TeV_pythia8',
    'root://cmseos.fnal.gov//store/user/lpcdarkqcd/boosted/umd/BKG/bkg_Mar25_year2018/Autumn18.QCD_Pt_470to600_TuneCP5_13TeV_pythia8',
    'root://cmseos.fnal.gov//store/user/lpcdarkqcd/boosted/umd/BKG/bkg_Apr26_year2018/Autumn18.QCD_Pt_470to600_TuneCP5_13TeV_pythia8',
    'root://cmseos.fnal.gov//store/user/lpcdarkqcd/boosted/umd/BKG/bkg_May04_year2018/Autumn18.QCD_Pt_470to600_TuneCP5_13TeV_pythia8',
    'root://cmseos.fnal.gov//store/user/lpcdarkqcd/boosted/umd/BKG/bkg_Mar25_year2018/Autumn18.QCD_Pt_600to800_TuneCP5_13TeV_pythia8',
    'root://cmseos.fnal.gov//store/user/lpcdarkqcd/boosted/umd/BKG/bkg_May04_year2018/Autumn18.QCD_Pt_600to800_TuneCP5_13TeV_pythia8',
    'root://cmseos.fnal.gov//store/user/lpcdarkqcd/boosted/umd/BKG/bkg_May07_year2018/Autumn18.QCD_Pt_600to800_TuneCP5_13TeV_pythia8',
    'root://cmseos.fnal.gov//store/user/lpcdarkqcd/boosted/umd/BKG/bkg_Mar25_year2018/Autumn18.QCD_Pt_800to1000_TuneCP5_13TeV_pythia8_ext1',
    'root://cmseos.fnal.gov//store/user/lpcdarkqcd/boosted/umd/BKG/bkg_May04_year2018/Autumn18.QCD_Pt_800to1000_TuneCP5_13TeV_pythia8_ext1',
    'root://cmseos.fnal.gov//store/user/lpcdarkqcd/boosted/umd/BKG/bkg_May07_year2018/Autumn18.QCD_Pt_800to1000_TuneCP5_13TeV_pythia8_ext1',
    'root://cmseos.fnal.gov//store/user/lpcdarkqcd/boosted/umd/BKG/bkg_Mar25_year2018/Autumn18.QCD_Pt_1000to1400_TuneCP5_13TeV_pythia8',
    'root://cmseos.fnal.gov//store/user/lpcdarkqcd/boosted/umd/BKG/bkg_May04_year2018/Autumn18.QCD_Pt_1000to1400_TuneCP5_13TeV_pythia8',
    'root://cmseos.fnal.gov//store/user/lpcdarkqcd/boosted/umd/BKG/bkg_May07_year2018/Autumn18.QCD_Pt_1000to1400_TuneCP5_13TeV_pythia8',

    ]]
bkg_rootfiles = list(itertools.chain.from_iterable(bkg_rootfiles))

bdt_json = 'svjbdt_girthreweight_Jan06.json' #girth re-weight


#print("background rootfiles are: ", bkg_rootfiles)

def submit_chunk(chunk):
    submit(
        rootfiles=chunk,
        bdt_json=bdt_json,
        run_env='condapack:root://cmseos.fnal.gov//store/user/klijnsma/conda-svj-bdt.tar.gz',
        transfer_files=['combine_hists.py', 'dataset.py', bdt_json],
        )


for chunk in qondor.utils.chunkify(bkg_rootfiles, chunksize=25):
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
           #'gsiftp://hepcms-gridftp.umd.edu//mnt/hadoop/cms/store/user/snabili/NPZfiles_PostBDT/BDT_QCD_03152022_'
           #'gsiftp://hepcms-gridftp.umd.edu//mnt/hadoop/cms/store/user/snabili/NPZfiles_PostBDT/BDT_QCD_03142022_dataStudy_'
           'gsiftp://hepcms-gridftp.umd.edu//mnt/hadoop/cms/store/user/snabili/UltraLegacy_npz/QCD_04102022_masseq_/'
           + '/'.join(rootfile.split('/')[-3:]).replace('.root', '.npz'),
           implementation='gfal', env=qondor.BARE_ENV),

    except Exception as e:
        print('Failed for rootfile ' + rootfile + ':')
        print(e)
        
    finally:
        if osp.isfile('out.npz'): os.remove('out.npz')
        if osp.isfile('in.root'): os.remove('in.root')
