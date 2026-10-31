Use your computer vision capabilities and knowledge of ECG annotation standards to produce an "Astra annotation" of this single-lead ECG strip. I would like to review it alongside other delineation methods.

The images and sample table show the same Polar H10 recording. Express uncertainty and leave landmarks null when you cannot identify them.

Output JSON with:
- method, overall_quality, convention: strings
- observations, unassessable: arrays of strings
- beats: ordered array of {fid_index, confidence, notes, marks}
Use a consistent QRS fiducial for fid_index. Confidence is high/medium/low; notes is a string. Each marks object has P_on, P_peak, P_off, QRS_on, Q_peak, R_peak, S_peak, QRS_off, T_on, T_peak, T_off. Each value is null or [best, earliest_plausible, latest_plausible] native sample indices. Python will calculate intervals from these indices.
