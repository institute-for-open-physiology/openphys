"""Numerical adapter checks; not a MATLAB-equivalence claim."""

from pathlib import Path
import shutil, subprocess
import numpy as np
import pywt
import pytest
from scipy.io import savemat, loadmat


@pytest.mark.skipif(
    not shutil.which("octave"), reason="Octave integration dependency unavailable"
)
def test_swt_impulse_ramp_noise_and_endpoint_extension(tmp_path):
    compat = Path(__file__).parents[1] / "octave_compat"
    for p in compat.glob("*.m"):
        shutil.copy2(p, tmp_path / p.name)
    filters = {}
    for name in ["haar", "rbio3.3"]:
        w = pywt.Wavelet(name)
        filters[name.replace(".", "_") + "_lo"] = w.dec_lo
        filters[name.replace(".", "_") + "_hi"] = w.dec_hi
    savemat(tmp_path / "wavelet_filters.mat", filters)
    rng = np.random.default_rng(20260925)
    x = np.vstack([np.eye(1, 256, 83)[0], np.arange(256) / 255, rng.normal(size=256)])
    savemat(tmp_path / "input.mat", {"x": x})
    # Use argv for paths, not interpolation into executable Octave code.
    script = """args=argv();addpath(args{1});load(fullfile(args{1},'input.mat'));
assert(isequal(wextend(1,'sp0',[1;2;3],2,'l'),[1;1;1;2;3]));
assert(isequal(wextend(1,'sp0',[1,2,3],2,'r'),[1,2,3,3,3]));
assert(isequal(wextend(1,'sp0',[1,2],1,'b'),[1,1,2,2]));
for k=1:3
 [a,d]=swt(x(k,:),6,'haar');save('-mat7-binary',fullfile(args{1},sprintf('haar%d.mat',k)),'a','d');
 [a,d]=swt(x(k,:),6,'rbio3.3');save('-mat7-binary',fullfile(args{1},sprintf('rbio%d.mat',k)),'a','d');
 [a,d]=swt(x(k,:),6,[1/8,3/8,3/8,1/8],[0,-2,2,0]);save('-mat7-binary',fullfile(args{1},sprintf('custom%d.mat',k)),'a','d');
end
"""
    (tmp_path / "check.m").write_text(script)
    subprocess.run(
        ["octave", "--quiet", str(tmp_path / "check.m"), str(tmp_path)],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    for prefix, w in [
        ("haar", "haar"),
        ("rbio", "rbio3.3"),
        (
            "custom",
            pywt.Wavelet(
                "custom",
                filter_bank=[
                    [1 / 8, 3 / 8, 3 / 8, 1 / 8],
                    [0, -2, 2, 0],
                    [0] * 4,
                    [0] * 4,
                ],
            ),
        ),
    ]:
        for k, row in enumerate(x, 1):
            mat = loadmat(tmp_path / f"{prefix}{k}.mat")
            expected = list(reversed(pywt.swt(row, w, level=6, norm=False)))
            for field, j in [("a", 0), ("d", 1)]:
                np.testing.assert_allclose(
                    mat[field],
                    np.stack([c[j] for c in expected]),
                    rtol=1e-12,
                    atol=1e-12,
                )
