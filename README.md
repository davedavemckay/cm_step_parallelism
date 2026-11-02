# LSSTCam DRP Campaign Step Parallelism (`cm_step_parallelism`)

This repository contains BPS configuration files, DAG workflow diagrams, and automated `tmux` orchestration scripts for testing and comparing **user-oriented submission parallelism** across the Vera C. Rubin Observatory **LSSTCam Data Release Production (DRP)** pipeline (`$DRP_PIPE_DIR/pipelines/LSSTCam/DRP.yaml`).

---

## Purpose of the Test

Although the LSSTCam DRP pipeline is organized by convention into four sequential stages (`stage1-single-visit`, `stage2-recalibrate`, `stage3-coadd`, and `stage4-measure-variability`) comprising 19 named steps and 153 tasks, the true data-dependency graph (`pipetask build --show pipeline-graph`) contains significant cross-stage and intra-stage independence.

When submitting DRP campaigns via `bps submit` (here configured for submission from **USDF at SLAC** to remote HTCondor execution at **LANCS** on tract `2234`, weekly `w_2026_44`), two practical constraints govern how steps can be grouped or split:
1. **Quantum Clustering (`$DRP_PIPE_DIR/bps/clustering/LSSTCam/DRP-clustering.yaml`):** 23 task clusters group tasks sharing dimensions (e.g., `visit, detector` or `tract, patch`) into single HTCondor jobs so intermediate products flow in-memory/on-node without round-tripping to the central Butler datastore. **Splitting a cluster across two BPS submissions is prohibited.**
2. **Worker-Local Datastore Caching (`$DRP_PIPE_DIR/bps/caching/LSSTCam/LANCS/DRP-caching.yaml`):** Heavy intermediate datasets (`preliminary_visit_image`, `visit_image`, `direct_warp`, `psf_matched_warp`, `deep_coadd_predetection`, `difference_image`, etc.) are cached on worker scratch disks only for the duration of a single clustered HTCondor job.

This repository compares two execution strategies that strictly preserve all 23 BPS quantum clusters and worker-local caching boundaries:

| Scenario | Subfolder | Output Collection | Waves | Description |
| :--- | :--- | :--- | :---: | :--- |
| **1. Baseline Step Parallelism** | [`pipelines_campaign/`](./pipelines_campaign/) | `u/dmckayuk/{lsstVersion}/tracking_test/pi_steps` | **8 Waves** | Uses the standard unedited campaign BPS YAML files, submitted in the maximum-parallelism wave order permitted by their true dataset dependencies. |
| **2. Clustering- & Caching-Aware Split Parallelism** | [`parallel_campaign/`](./parallel_campaign/) | `u/dmckayuk/{lsstVersion}/tracking_test/pll_steps` | **9 Waves** | Splits monolithic steps (`step1c_1d`, `step3a2-coadds`, `step4a`, `step4c`) strictly along disconnected task subgraphs and zero-cache boundaries to unblock critical-path steps earlier. |

A complete technical reference of all 19 DRP steps, 153 tasks, downstream-first prerequisite tables, clustering definitions, and caching boundaries is provided in [`lsst_drp_pipeline_dependencies_and_state_context_v2.md`](./lsst_drp_pipeline_dependencies_and_state_context_v2.md).

---

## Scenario 1: Baseline Campaign (`pipelines_campaign/`)

Orchestrated by [`pipelines_campaign/run_baseline_campaign.sh`](./pipelines_campaign/run_baseline_campaign.sh) ([SVG diagram](./pipelines_campaign/mermaid-diagram.svg)):

- **Wave 1:** `step1a-step1b.yaml`
- **Wave 2 (Parallel):** `step1c_step1d.yaml` + `step2c.yaml` *(GBDES astrometry only depends on `step1a-step1b`, not `step1c_step1d` or `step2a`)*
- **Wave 3 (Parallel with `step2c`):** `step2a.yaml` *(FGCM global photometry)*
- **Wave 4:** `step2d.yaml` *(Per-visit recalibration & PSF refit)*
- **Wave 5 (3-Way Parallel):** `step3a1-warps.yaml` + `step2e-step2f.yaml` + `step4b.yaml` *(`generateEphemerides` and warps only need `visit_summary` from `step2d`)*
- **Wave 6:** `step3a2-coadds.yaml` *(Coadd assembly + Scarlet deblending + MultiProFit + `Object` catalog)*
- **Wave 7 (Parallel):** `step4a.yaml` + `step3b.yaml` *(Global coadd summary runs as a parallel leaf)*
- **Wave 8:** `step4c.yaml` *(DIA association, forced photometry, and Pretty Coadds)*

---

## Scenario 2: Improved Parallel Campaign (`parallel_campaign/`)

Orchestrated by [`parallel_campaign/run_parallel_campaign.sh`](./parallel_campaign/run_parallel_campaign.sh) ([SVG diagram](./parallel_campaign/mermaid-diagram.svg)):

### Key Bottlenecks Removed
1. **Early FGCM (`step2a.yaml`) Start:** Splits `step1c1-isolated-stars.yaml` (`associateIsolatedStar`) from the 60 GB QA task in `step1c2-step1d2-qa.yaml` and runs `step1d1-visit-tables.yaml` in Wave 2 so `step2a.yaml` launches immediately in Wave 3 without waiting for Stage 1 QA plots.
2. **Wave 5 Visit Reprocessing & Sky Correction (`step4a1-reprocess-skycorr.yaml`):** Runs `skyCorrCluster` and `step4a_reprocess_standardize` (producing final `visit_image` and `skyCorr`) in **Wave 5** simultaneously with `step3a1-warps.yaml`, rather than waiting until after coadds finish.
3. **Parallel Difference Imaging (`step4a2-diffim.yaml`) & Multiband Fitting (`step3a3-multiband-objects.yaml`):** Keeps all coadd image assembly (`assembleDeepCoadd`, `assembleCellCoadd`, `assembleTemplateCoadd`) together in `step3a2-coadd-images.yaml` (Wave 6) to preserve `psf_matched_warp` caching, then immediately launches `step4a2-diffim.yaml` and `step4c1-pretty-coadds.yaml` in **Wave 7** in parallel with Scarlet deblending and MultiProFit galaxy fitting (`step3a3-multiband-objects.yaml`).
4. **4-Way Split of `step4c`:** Runs `step4c2-source-qa.yaml` in **Wave 6**, `step4c1-pretty-coadds.yaml` in **Wave 7**, and `step4c3-dia-assoc-forced.yaml` + `step4c4-object-forced.yaml` in **Wave 8** (followed by `step4d.yaml` in **Wave 9**).

---

## Running the Campaigns in `tmux`

Both orchestrators poll `bps status`, run `bps submit` asynchronously in the background (so QuantumGraph generation never blocks polling or parallel submissions), persist state in `.campaign_state/campaign_state.tsv`, and automatically switch failed steps to retry mode (`--skip-existing-in {output} --clobber-outputs`) upon retry.

### Launch Baseline Campaign (`pipelines_campaign/`)
```bash
./pipelines_campaign/run_baseline_campaign.sh --tmux
# Attach to live dashboard:
tmux attach -t drp_baseline
```

### Launch Parallel Campaign (`parallel_campaign/`)
```bash
./parallel_campaign/run_parallel_campaign.sh --tmux
# Attach to live dashboard:
tmux attach -t drp_parallel
```

### Retry Handling (`extraQgraphOptions`)
- **First try:** Each YAML uses its primary `extraQgraphOptions: "--dataset-query-constraint ..."` line while keeping `# extraQgraphOptions: "... --skip-existing-in {output} --clobber-outputs"` commented out with `# `.
- **Retries:** When a step fails and is retried (via `--resubmit-retries <N>`, `--retry <yaml>`, or by re-running the orchestrator after a failure), the script comments out the first line with `# ` and uncomments the second line (`--skip-existing-in {output} --clobber-outputs`) so completed quanta are not re-executed.
- **Reset:** Passing `--reset` clears `.campaign_state/` and restores all YAML files to their first-try `extraQgraphOptions` state.

---

## License

Licensed under the [Apache License, Version 2.0](./LICENSE).
