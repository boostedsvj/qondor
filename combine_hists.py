import os, os.path as osp, uuid, multiprocessing, shutil, logging
from time import strftime

import numpy as np
import xgboost as xgb
import seutils
import uptools
uptools.logger.setLevel(logging.WARNING)

from dataset import preselection, get_subl, calculate_mt_rt, CutFlowColumn, part_flavor, Offset_Constituents, calculate_mass, calculate_mt, calc_dphi, calculate_massmet, calculate_massmetpz, calculate_massmetpzm

def get_scores(rootfile, model):
    '''
    Worker function that reads a single rootfile and returns the
    bdt score, and a few event-level variables to be potentially used
    for histogramming.
    Only uses events that pass the preselection.
    '''
    X = []
    X_histogram = []
    X_mass = []
    X_allvar = []
    cutflow = CutFlowColumn()
    try:
        for event in uptools.iter_events(rootfile):
            cutflow.plus_one('total')
            if not preselection(event, cutflow): continue
            subl = get_subl(event)
            subl.genmt, subl.genrt = calculate_mt_rt(subl, event[b'GenMET'], event[b'GenMETPhi'])
            subl.mt, subl.rt = calculate_mt_rt(subl, event[b'MET'], event[b'METPhi'])
            genmet = event[b'GenMET']
            met = event[b'MET']
            subl.mass = calculate_mass(subl)
            subl.massgenmet = calculate_massmet(subl, event[b'GenMET'], event[b'GenMETPhi'])
            subl.massgenmetpz = calculate_massmetpz(subl, event[b'GenMET'], event[b'GenMETPhi'])
            subl.massgenmetpzm = calculate_massmetpzm(subl, event[b'GenMET'], event[b'GenMETPhi'])
            subl.massmet = calculate_massmet(subl, event[b'MET'], event[b'METPhi'])
            subl.massmetpz = calculate_massmetpz(subl, event[b'MET'], event[b'METPhi'])
            subl.massmetpzm = calculate_massmetpzm(subl, event[b'MET'], event[b'METPhi'])
            trig = event[b'JetsAK8.fCoordinates.fPt'][0] 
            subl.dphi = calc_dphi(subl.phi, event[b'METPhi'])
            subl.gendphi = calc_dphi(subl.phi, event[b'GenMETPhi'])
            X.append([
                subl.girth, subl.axisminor, subl.ecfM2b1, subl.ecfD2b1, subl.ecfC2b1,
                subl.ecfN2b2, subl.metdphi, subl.ptD, subl.multiplicity,
                ])
            X_histogram.append([subl.mt, subl.genmt, subl.rt, subl.genrt, subl.pt, subl.energy])
            X_allvar.append([
                subl.girth, subl.axisminor, subl.ecfM2b1, subl.ecfD2b1, subl.ecfC2b1,
                subl.ecfN2b2, subl.metdphi, subl.ptD, subl.multiplicity, trig, subl.dphi, subl.gendphi, subl.eta, subl.pz, subl.mass, subl.massmet, subl.massmetpz, subl.massmetpzm, subl.massgenmet, subl.massgenmetpz, subl.massgenmetpzm, met, genmet
                ])
    except IndexError:
        print(f'Problem with {rootfile}; saving {cutflow["preselection"]} good entries')
    except:
        print(f'Error processing {rootfile}; Skipping')
    if cutflow['preselection'] == 0:
        print(f'0/{cutflow["total"]} events passed the preselection for {rootfile}')
        return
    # Get the bdt scores
    score = model.predict_proba(np.array(X))[:,1]
    # Prepare and dump to file
    X_histogram = np.array(X_histogram)
    X_allvar = np.array(X_allvar)
    return dict(
        score=score,
        **{key: X_histogram[:,index] for index, key in enumerate(['mt', 'genmt', 'rt', 'genrt', 'pt', 'energy'])},
        **{key: X_allvar[:,index] for index, key in enumerate(['girth', 'axisminor', 'ecfM2b1', 'ecfD2b1', 'ecfC2b1', 'ecfN2b2', 'metphi', 'ptD', 'multiplicity', 'trig', 'dphi', 'gendphi', 'eta', 'pz', 'mass', 'massmet', 'massmetpz', 'massmetpzm', 'massgenmet', 'massgenmetpz', 'massgenmetpzm', 'met', 'genmet'])},
        **cutflow.counts
        )





def dump_score_npz(rootfile, model, outfile):
    '''    
    Calculates score and dumps events that pass the preselection to a .npz file.
    '''
    d = get_scores(rootfile, model)
    print(f'Dumping {len(d["score"])} events from {rootfile} to {outfile}')
    outdir = osp.dirname(outfile)
    if outdir and not osp.isdir(outdir): os.makedirs(outdir)
    np.savez(outfile, **d)


def combine_ds(ds):
    """
    Takes a iterable of dict-like objects with the same keys,
    and combines them in a single dict.

    Arrays will be concatenated, scalar values will be summed
    """
    combined = {}
    for d in ds:
        for key, value in d.items():
            if not key in combined: combined[key] = []
            if value.shape and len(value) == 0: continue
            combined[key].append(value)
    # Make proper np arrays
    for key, values in combined.items():
        if len(values[0].shape) == 0:
            combined[key] = np.array(values).sum()
        else:
            combined[key] = np.concatenate(values)
    return combined


def combine_npzs(npzs):
    """
    Like combine_ds, but instead takes an iterable of npz files
    """
    return combine_ds((np.load(npz) for npz in npzs))


def try_import_ROOT():
    try:
        import ROOT
    except ImportError:
        print(
            'ROOT is required to be installed for this operation. Run:\n'
            'conda install -c conda-forge root'
            )
        raise


def combine_ds_with_weights(ds, weights):
    """
    Combines several dicts into a single dict, with weights
    """
    if len(ds) != len(weights): raise ValueError('len ds != len weights')
    counts = [ len(d['score']) for d in ds ]
    optimal_counts = optimal_count(counts, weights)

    # Purely for debugging:
    print('Counts:')
    for i, (count, opt_count) in enumerate(zip(counts, optimal_counts)):
        print(f'{i} : {count:8} available, using {opt_count}')

    # Combine from an iterator with the dicts cut to size
    return combine_ds(( shrink_dict(d, opt_count) for d, opt_count in zip(ds, optimal_counts) ))


def shrink_dict(d, n):
    """
    Slices all values that are arrays in d up to :n.
    Integer counts are reduced by the fraction n/len(d)
    """
    len_d = len(d['score']) # Just pick an array key that is always there
    frac = min(float(n/len_d), 1.)
    return { k : v[:n] if v.shape else frac*v for k, v in d.items()}


# Defauly mt binning
MT_BINNING = [160.+8.*i for i in range(44)]
# MT_BINNING = [8.*i for i in range(130)]


def make_mt_histogram(name, mt, score=None, threshold=None, mt_binning=None, normalization=None):
    """
    Dumps the mt array to a TH1F. If `score` and `threshold` are supplied, a
    cut score>threshold will be applied.

    Normalization refers to the normalization *before* applying the threshold!
    """
    try_import_ROOT()
    import ROOT
    from array import array
    efficiency = 1.
    if threshold is not None:
        mt = mt[score > threshold]
        efficiency = (score > threshold).sum() / score.shape[0]
        # print(f'{name}: {efficiency=}')
    binning = array('f', MT_BINNING if mt_binning is None else mt_binning)
    h = ROOT.TH1F(name.replace('.','p'), name, len(binning)-1, binning)
    ROOT.SetOwnership(h, False)
    [ h.Fill(x) for x in mt ]
    if normalization is not None:
        integral = h.Integral(0, h.GetNbinsX()+1)
        h.Scale(normalization*efficiency / integral if integral != 0. else 0.)
    return h


def make_summed_histogram(name, ds, norms, threshold=None, mt_binning=None):
    from operator import add
    from functools import reduce
    h = reduce(add, (
        make_mt_histogram(
            str(uuid.uuid4()), d['mt'], d['score'],
            threshold=threshold, normalization=norm, mt_binning=mt_binning
            )
        for d, norm in zip(ds, norms)
        ))
    h.SetNameTitle(name, name)
    return h


def optimal_count(counts, weights):
    """
    Given an array of counts and an array of desired weights (e.g. cross sections),
    find the highest possible number of events without underrepresenting any bin
    """
    # Normalize weights to 1.
    weights = np.array(weights) / sum(weights)

    # Compute event fractions
    counts = np.array(counts)
    n_total = np.sum(counts)
    fractions = counts / n_total

    imbalance = weights / fractions
    i_max_imbalance = np.argmax(imbalance)
    max_imbalance = imbalance[i_max_imbalance]

    if max_imbalance == 1.:
        # Counts array is exactly balanced; Don't do anything
        return counts

    n_target = counts[i_max_imbalance] / weights[i_max_imbalance]
    optimal_counts = (weights * n_target).astype(np.int32)
    return optimal_counts


# ________________________________________________________
# For local multiprocessing running
# Not recommended, way too slow

def dump_score_npz_worker(input):
    '''    
    Like dump_score_npz but takes a single tuple as input.
    To be used in dump_score_npzs_mp.
    '''
    dump_score_npz(*input)

def dump_score_npzs_mp(model, rootfiles, outfile, n_threads=12, keep_tmp_files=False):
    '''
    Entrypoint to read a list of rootfiles, locally compute the BDT scores, and combine
    it all in a single .npz file.
    Uses multiprocessing to speed things up.
    '''
    print(f'Processing {len(rootfiles)} rootfiles to {outfile}')
    tmpdir = strftime(f'TMP_%b%d_%H%M%S_{outfile}')
    os.makedirs(tmpdir)
    # Prepare input data
    data = []
    for rootfile in rootfiles:
        data.append([ rootfile, model, osp.join(tmpdir, str(uuid.uuid4())+'.npz') ])
    # Process data in multiprocessing pool
    # Every thread will dump data into tmpdir/<unique id>.npz
    pool = multiprocessing.Pool(n_threads)
    pool.map(dump_score_npz_worker, data)
    pool.close()
    pool.join()
    # Combine the tmpdir/<unique id>.npz --> outfile and remove tmp files
    combine_npzs(tmpdir, outfile)
    if not keep_tmp_files:
        print(f'Removing {tmpdir}')
        shutil.rmtree(tmpdir)

# ________________________________________________________
# Some tests

def test_get_scores():
    model = xgb.XGBClassifier()
    model.load_model('/Users/klijnsma/work/svj/bdt/svjbdt_Aug02.json')
    rootfile = 'TREEMAKER_genjetpt375_Jul21_mz250_mdark10_rinv0.337.root'
    d = get_scores(rootfile, model)
    import pprint
    pprint.pprint(d, sort_dicts=False)

def test_dump_score_npz_worker():
    model = xgb.XGBClassifier()
    model.load_model('/Users/klijnsma/work/svj/bdt/svjbdt_Aug02.json')
    rootfile = 'TREEMAKER_genjetpt375_Jul21_mz250_mdark10_rinv0.337.root'
    dump_score_npz_worker((rootfile, model, 'out.npz'))

def test_dump_score_npzs_mp():
    model = xgb.XGBClassifier()
    model.load_model('/Users/klijnsma/work/svj/bdt/svjbdt_Aug02.json')
    rootfiles = seutils.ls_wildcard(
        'root://cmseos.fnal.gov//store/user/lpcdarkqcd/MCSamples_Summer21/TreeMaker'
        '/genjetpt375_mz250_mdark10_rinv0.3/*.root'
        )
    outfile = 'mz250_mdark10_rinv0p3.npz'
    # dump_score_npz_worker((rootfiles[1], model, 'out.npz'))
    dump_score_npzs_mp(model, rootfiles, outfile)

def test_optimal_count():
    counts = np.array([ 100, 200, 150 ])
    np.testing.assert_almost_equal(
        optimal_count(counts, [1./3, 1./3, 1./3]),
        np.array([ 100, 100, 100 ])
        )
    np.testing.assert_almost_equal(
        optimal_count(counts, [.2, .4, .4]),
        np.array([ 75, 150, 150 ])
        )
    np.testing.assert_almost_equal(
        optimal_count(counts, counts/counts.sum()),
        counts
        )
    np.testing.assert_almost_equal(
        optimal_count(
            [18307, 104484, 366352, 242363, 163624],
            [136.52, 278.51, 150.96, 26.24, 7.49]
            ),
        [18307, 37347, 20243, 3518, 1004]
        )
    print('Succeeded')


def test_sum_hists():
    import ROOT
    h1 = ROOT.TH1F('h1', 'h1', 10, array('f', np.linspace(200., 400., 11)))
    h2 = ROOT.TH1F('h2', 'h2', 10, array('f', np.linspace(200., 400., 11)))
    h1.Fill(250.)
    h1.Fill(300.)
    h2.Fill(250.)
    h2.Fill(350.)
    def printh(h):
        contents = []
        for i in range(h.GetNbinsX()):
            contents.append(h.GetBinContent(i+1))
        print(np.array(contents))
    printh(h1)
    printh(h2)
    printh(h1+h2)
    from operator import add
    from functools import reduce
    h_sum = reduce(add, [h1, h2])
    printh(h_sum)
    printh(h1)
    printh(h2)

if __name__ == '__main__':
    test_get_scores()
