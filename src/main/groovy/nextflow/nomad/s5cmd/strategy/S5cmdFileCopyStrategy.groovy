/*
 * Copyright 2026-, Incremental Steps Software Solutions OÜ
 *
 * This program and the accompanying materials are made available under
 * the terms of the Eclipse Public License 2.0 which is available at
 *
 *     https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 */
package nextflow.nomad.s5cmd.strategy

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.executor.ScriptFileCopyStrategy
import nextflow.executor.SimpleFileCopyStrategy
import nextflow.nomad.s5cmd.config.S5cmdConfig

import java.nio.file.Files
import java.nio.file.Path

/**
 * {@link ScriptFileCopyStrategy} that wires {@code s5cmd cp} into the
 * generated {@code .command.run} so worker containers stage their inputs
 * from S3 (via the per-task remote dir the operator pre-populated in
 * {@code S5cmdNomadInterop.prepare}) and rely on the bootstrap script
 * to push outputs back wholesale at task end.
 *
 * <h3>Stage-in</h3>
 * <p>For each {@code (stageName, sourcePath)} in
 * {@code task.getInputFilesMap()}, the operator side has already uploaded
 * the file to {@code s3://<bucket>/<prefix>/<NN>/<HASH>/inputs/<stageName>}.
 * This strategy emits a worker-side {@code s5cmd cp} that pulls each one
 * back into the worker's CWD as {@code ./<stageName>} — exactly what the
 * default {@link SimpleFileCopyStrategy} does with {@code cp -L}, but
 * cross-host via S3.</p>
 *
 * <h3>Stage-out</h3>
 * <p>Returns a no-op comment. {@code S5cmdNomadInterop}'s bootstrap script
 * pushes the entire task dir back via a single {@code s5cmd cp ./ "$NXF_S5CMD_REMOTE_WORKDIR"}
 * after the task exits, then the operator's {@code synchronizeCompletion()}
 * pulls that bundle down to {@code task.workDir}. So per-output staging
 * inside {@code .command.run} would just duplicate work.</p>
 *
 * <p>NOTE: outputs published outside the task work dir (e.g. via
 * {@code publishDir}) still flow through Nextflow's normal post-task
 * publishing — that path uses {@code task.workDir.resolve(name)} after
 * the operator-side sync, so it works without extra strategy logic.</p>
 */
@Slf4j
@CompileStatic
class S5cmdFileCopyStrategy implements ScriptFileCopyStrategy {

    private final S5cmdConfig config
    private final S5cmdCommandBuilder cmdBuilder
    private final SimpleFileCopyStrategy fallback

    /** S3 URL of the per-task remote dir with trailing slash, e.g. {@code s3://bucket/prefix/NN/HASH/}. */
    private final String remoteTaskDirUrl

    S5cmdFileCopyStrategy(S5cmdConfig config,
                          Path workDir,
                          Path targetDir,
                          String stageInMode,
                          String stageOutMode,
                          String remoteTaskDirUrl) {
        this.config = config
        this.cmdBuilder = new S5cmdCommandBuilder(config)
        this.remoteTaskDirUrl = remoteTaskDirUrl?.endsWith('/') ? remoteTaskDirUrl : (remoteTaskDirUrl + '/')
        this.fallback = new SimpleFileCopyStrategy()
        this.fallback.workDir = workDir
        this.fallback.targetDir = targetDir
        this.fallback.stageinMode = stageInMode
        this.fallback.stageoutMode = stageOutMode
    }

    /** S3 URL of the per-task inputs dir, trailing-slashed. */
    String inputsBaseUrl() {
        return remoteTaskDirUrl + 'inputs/'
    }

    @Override
    String getBeforeStartScript() {
        // Export the same AWS_*/S3_* envs the worker bootstrap exports — so
        // the stage-in s5cmd calls below have credentials regardless of what
        // the docker image inherits. Then chain whatever fallback wants.
        String exports = cmdBuilder.envExports()
        String base = fallback.getBeforeStartScript() ?: ''
        StringBuilder sb = new StringBuilder()
        sb.append('# nf-s5cmd: stage-in/out env\n')
        if( exports ) { sb.append(exports); sb.append('\n') }
        if( base ) { sb.append(base); sb.append('\n') }
        return sb.toString()
    }

    @Override
    String getStageInputFilesScript(Map<String, Path> inputFiles) {
        if( !inputFiles ) return ''
        StringBuilder sb = new StringBuilder()
        sb.append('# nf-s5cmd: stage-in (s5cmd cp from per-task remote dir)\n')
        for( entry in inputFiles.entrySet() ) {
            String stageName = entry.key
            java.nio.file.Path source = entry.value
            // Directories were uploaded with the recursive form by the operator
            // side (S5cmdNomadInterop.uploadInputFiles); mirror that here so
            // the worker pulls every file under the prefix into ./<stageName>/.
            String cmd
            if( source != null && java.nio.file.Files.isDirectory(source) ) {
                String src = inputsBaseUrl() + stageName + '/'
                String dst = './' + stageName + '/'
                cmd = cmdBuilder.buildCopyDir(src, dst, false)
            }
            else {
                String src = inputsBaseUrl() + stageName
                String dst = './' + stageName
                cmd = cmdBuilder.buildCopy(src, dst, false)
            }
            sb.append(cmd)
            sb.append('\n')
        }
        return sb.toString()
    }

    @Override
    String getUnstageOutputFilesScript(List<String> outputFiles, Path targetDir) {
        // Outputs are pushed back by S5cmdNomadInterop's bootstrap script
        // (s5cmd cp ./ "$NXF_S5CMD_REMOTE_WORKDIR") after the task exits.
        // No per-file unstage needed inside .command.run.
        return '# nf-s5cmd: outputs pushed back by bootstrap (recursive s5cmd cp)\n'
    }

    @Override String touchFile(Path path) { fallback.touchFile(path) }
    @Override String fileStr(Path path) { fallback.fileStr(path) }

    /**
     * Called by Nextflow's BashWrapperBuilder to render the {@code publishDir}
     * copy step inside {@code .command.run}. We intercept ONLY when the user
     * has explicitly opted in by listing the destination prefix in
     * {@code s5cmd.publishPaths} — otherwise Nextflow's native publishDir-via-
     * nf-amazon handles the transfer.
     *
     * <p>Why opt-in (and separate from {@code s5cmd.paths}):</p>
     * <ul>
     *   <li>Nextflow's nf-amazon already handles s3 publishDir natively.
     *       Intercepting silently would duplicate that flow and force
     *       co-existence (nf-amazon does mkdir / list, we do cp) — a
     *       confusing seam that broke pipelines reading from real public S3
     *       (e.g. {@code s3://ngi-igenomes/}) when {@code aws.client} was
     *       wired to a private endpoint.</li>
     *   <li>Input staging and output publishing are independent decisions.
     *       A pipeline may want fast s5cmd input staging from one bucket
     *       AND vanilla Nextflow publishing to another.</li>
     * </ul>
     *
     * <p>When opted in, this is a meaningful speedup: s5cmd's worker-pool +
     * multipart upload is 5-10× faster than nf-amazon's single-stream Java
     * client on large objects.</p>
     *
     * <p>{@code mode = 'move'} semantics: even when intercepting, we emit
     * {@code s5cmd cp} (not {@code mv}) and let Nextflow's publishDir handler
     * issue the local {@code rm} after a successful copy. Opt-in
     * {@code s5cmd mv} is tracked in the improvements roadmap.</p>
     */
    @Override
    String copyFile(String source, Path target) {
        String dst = renderTargetUrl(target)
        if( dst != null && config.matchesPublish(dst) ) {
            // Detect dir-vs-file from local source path; on the worker the
            // file/dir has just been written by the user task, so the local
            // FS check is authoritative.
            try {
                Path src = Path.of(source)
                if( Files.isDirectory(src) ) {
                    return cmdBuilder.buildCopyDir(source, dst.endsWith('/') ? dst : dst + '/', true)
                }
            } catch (Throwable ignored) {
                // fall through to single-file copy
            }
            return cmdBuilder.buildCopy(source, dst, true)
        }
        return fallback.copyFile(source, target)
    }

    /**
     * Render a {@link Path} to its URL form for prefix matching. Nextflow's
     * S3 paths come through as {@code S3Path} (from nf-amazon) which
     * stringifies to {@code /bucket/key}; we need {@code s3://bucket/key}.
     * Local paths return null (caller falls back to default behaviour).
     */
    private static String renderTargetUrl(Path target) {
        if( target == null ) return null
        try {
            URI u = target.toUri()
            String scheme = u?.scheme
            if( scheme == 's3' || scheme == 's3a' ) {
                // toUri() on S3Path gives s3:///bucket/key (extra slash); normalise
                return scheme + '://' + (u.host ?: '') + u.path
            }
        } catch (Throwable ignored) { /* not a URI-able path */ }
        return null
    }

    @Override String exitFile(Path file) { fallback.exitFile(file) }
    @Override String pipeInputFile(Path file) { fallback.pipeInputFile(file) }
    @Override String getEnvScript(Map environment, boolean container) { fallback.getEnvScript(environment, container) }
}
