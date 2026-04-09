pipeline {
    agent { label "db-sync-agent" }
    // agent cần: Python 3.9+, oracledb, pyodbc, paramiko

    parameters {
        string(name: "TARGET_SYSTEM",     defaultValue: "ALL")
        string(name: "TARGET_ENV",        defaultValue: "ALL")
        booleanParam(name: "DRY_RUN",     defaultValue: false)
        booleanParam(name: "FORCE_APPLY", defaultValue: false)
        string(name: "NOTIFY_CHANNEL",    defaultValue: "#db-ops")
    }

    triggers { cron("H 2 * * *") }

    options {
        skipDefaultCheckout(true)
        buildDiscarder(logRotator(numToKeepStr: "30", artifactNumToKeepStr: "90"))
        timeout(time: 2, unit: "HOURS")
        timestamps()
    }

    stages {

        // ── STAGE 1: Checkout ─────────────────────────────────────────────────
        stage("Checkout") {
            steps {
                cleanWs()
                checkout([
                    $class: "GitSCM",
                    branches: [[name: "*/main"]],
                    userRemoteConfigs: [[
                        url: "https://github.com/TuanPM9609/infra-config-mgmt.git",
                        credentialsId: "gitlab-jenkins-token"
                    ]],
                    extensions: [
                        [$class: "CleanBeforeCheckout"],
                        [$class: "CloneOption", shallow: true, depth: 1]
                    ]
                ])
                sh "mkdir -p tmp reports"
                echo "Workspace: ${WORKSPACE}"
                echo "Git commit: \$(git rev-parse --short HEAD)"
            }
        }

        // ── STAGE 2: Validate ─────────────────────────────────────────────────
        stage("Validate params") {
            steps { script {
                if (params.TARGET_ENV == "prod")
                    error("[BLOCKED] TARGET_ENV=prod bị chặn cứng.")
                if (params.FORCE_APPLY && params.TARGET_ENV == "test")
                    error("[BLOCKED] FORCE_APPLY=true không được phép cho TARGET_ENV=test.")
                echo "TARGET_SYSTEM=${params.TARGET_SYSTEM} | TARGET_ENV=${params.TARGET_ENV}"
                echo "DRY_RUN=${params.DRY_RUN} | FORCE_APPLY=${params.FORCE_APPLY}"
            }}
        }

        // ── STAGE 3: Resolve ─────────────────────────────────────────────────
        stage("Resolve systems") {
            steps { script {
                def reg = readYaml file: "inventory/db_registry.yaml"
                env.SYSTEMS_JSON = groovy.json.JsonOutput.toJson(
                    resolveSystems(params.TARGET_SYSTEM, reg)
                )
                echo "Systems: ${env.SYSTEMS_JSON}"
            }}
        }

        // ── STAGE 4: Parallel sync ───────────────────────────────────────────
        stage("Parallel sync") {
            steps { script {
                def systems = readJSON text: env.SYSTEMS_JSON
                def envs    = (params.TARGET_ENV == "ALL") ? ["dev", "test"] : [params.TARGET_ENV]
                def jobs    = [:]

                systems.each { sys ->
                    envs.each { tgtEnv ->
                        def sysId  = sys.system_id
                        def jobEnv = tgtEnv

                        // ── withCredentials: inject tất cả credentials cho system này ──
                        // Mỗi system trong db_registry.yaml có credential_id cho db và os
                        // của từng env. Cần inject tất cả trước khi chạy parallel.
                        // Convention: credential_id "ora-prod-cred" → biến ORA_PROD_CRED_USR/PSW
                        //
                        // Cách đơn giản nhất: đặt credentials trong db_registry.yaml
                        // rồi build danh sách credentials động ở đây.
                        def envCfg   = sys.environments[jobEnv]
                        def prodCfg  = sys.environments["prod"]

                        def credBindings = buildCredBindings(sysId, jobEnv, prodCfg, envCfg)

                        jobs["${sysId}-${jobEnv}"] = {
                            withCredentials(credBindings) {

                                // 4a. Collect prod (baseline source — read-only)
                                sh """
                                    python scripts/collect.py \
                                      --system ${sysId} --env prod \
                                      --output tmp/${sysId}_prod_state.yaml
                                """

                                // 4b. Collect target env
                                sh """
                                    python scripts/collect.py \
                                      --system ${sysId} --env ${jobEnv} \
                                      --output tmp/${sysId}_${jobEnv}_state.yaml
                                """

                                // 4c. Diff
                                sh """
                                    python scripts/diff.py \
                                      --system ${sysId} --env ${jobEnv} \
                                      --output tmp/${sysId}_${jobEnv}_diff.yaml
                                """

                                if (!params.DRY_RUN) {
                                    if (!params.FORCE_APPLY) {
                                        approveIfNeeded(sys, jobEnv)
                                    }

                                    // 4d. Apply (prod guard cứng trong apply.py)
                                    sh """
                                        python scripts/apply.py \
                                          --system ${sysId} --env ${jobEnv}
                                    """

                                    // 4e. Verify
                                    sh """
                                        python scripts/verify.py \
                                          --system ${sysId} --env ${jobEnv}
                                    """
                                }
                            } // end withCredentials
                        }   // end job closure
                    }
                }
                parallel jobs
            }}
        }

        // ── STAGE 5: Report ──────────────────────────────────────────────────
        stage("Report") {
            steps {
                sh "python scripts/report.py --run-id ${BUILD_NUMBER}"
                archiveArtifacts artifacts: "reports/*.html", fingerprint: true
                script {
                    def msg = buildSlackSummary()
                    slackSend channel: params.NOTIFY_CHANNEL,
                              color: currentBuild.result == "SUCCESS" ? "good" : "danger",
                              message: msg
                }
            }
        }

    } // end stages

    // ── post { always } ──────────────────────────────────────────────────────
    post {
        always {
            // Bước 1: archive tmp — lưu để debug (chạy TRƯỚC cleanWs)
            archiveArtifacts artifacts: "tmp/**/*.yaml", allowEmptyArchive: true

            // Bước 2: cleanup workspace
            // tmp/*_state.yaml chứa thông tin cấu hình DB/OS nhạy cảm
            // → BẮT BUỘC xóa kể cả khi pipeline fail hoặc abort
            cleanWs(
                cleanWhenSuccess: true,
                cleanWhenFailure: true,
                cleanWhenAborted: true,
                deleteDirs: true,
                patterns: [
                    [pattern: ".git/**", type: "EXCLUDE"]  // giữ .git cho incremental checkout
                ]
            )
        }
        failure {
            slackSend channel: params.NOTIFY_CHANNEL, color: "danger",
                      message: ":x: DB Config Sync FAILED — Build #${BUILD_NUMBER}\n${BUILD_URL}console"
        }
    }

} // end pipeline


// ─── Helper functions ─────────────────────────────────────────────────────────

def resolveSystems(target, registry) {
    def all = registry.systems
    if (target == "ALL") return all
    if (target.startsWith("type:")) {
        def t = target.split(":")[1]
        return all.findAll { it.db_type == t }
    }
    def ids = target.split(",").collect { it.trim() }
    return all.findAll { ids.contains(it.system_id) }
}

/**
 * Build danh sách credential bindings cho withCredentials().
 *
 * Mỗi credential_id trong db_registry.yaml được map thành 1 binding:
 *   - DB credential → usernamePassword (inject USR + PSW)
 *   - OS/SSH credential → sshUserPrivateKey (inject USR + keyFile path vào PSW)
 *
 * Convention tên biến: credential_id.toUpperCase().replace("-","_") + "_USR/_PSW"
 * Ví dụ: "ora-prod-cred" → ORA_PROD_CRED_USR, ORA_PROD_CRED_PSW
 *         "ora-prod-ssh"  → ORA_PROD_SSH_USR,  ORA_PROD_SSH_PSW (path key file)
 */
def buildCredBindings(sysId, jobEnv, prodCfg, envCfg) {
    def bindings = []
    def seen     = [] as Set  // tránh duplicate binding cùng credential_id

    // Helper: thêm usernamePassword binding
    def addDbCred = { credId ->
        if (seen.contains(credId)) return
        seen << credId
        def prefix = credId.toUpperCase().replace("-", "_")
        bindings << usernamePassword(
            credentialsId:    credId,
            usernameVariable: "${prefix}_USR",
            passwordVariable: "${prefix}_PSW"
        )
    }

    // Helper: thêm SSH key binding
    def addSshCred = { credId ->
        if (seen.contains(credId)) return
        seen << credId
        def prefix = credId.toUpperCase().replace("-", "_")
        bindings << sshUserPrivateKey(
            credentialsId:    credId,
            usernameVariable: "${prefix}_USR",
            keyFileVariable:  "${prefix}_PSW"   // ← inject path key file vào PSW
        )
    }

    // Prod DB + OS credentials (cần để collect prod baseline)
    if (prodCfg?.db?.credential_id) addDbCred(prodCfg.db.credential_id)
    if (prodCfg?.os?.credential_id) addSshCred(prodCfg.os.credential_id)

    // Target env DB + OS credentials (cần để collect + apply)
    if (envCfg?.db?.credential_id) addDbCred(envCfg.db.credential_id)
    if (envCfg?.os?.credential_id) addSshCred(envCfg.os.credential_id)

    return bindings
}

def approveIfNeeded(sys, env) {
    if (sys.tier == "critical" || env == "test") {
        slackSend channel: "#dba-approval",
                  message: ":bell: Cần duyệt: ${sys.name} → ${env}\nDiff: ${BUILD_URL}artifact/tmp/${sys.system_id}_${env}_diff.yaml"
        input message: "Approve apply ${sys.system_id} → ${env}?",
              ok: "Approve",
              submitter: "dba-lead,tech-lead",
              submitterParameter: "APPROVED_BY"
    }
}

def buildSlackSummary() {
    def drifts  = sh(script: "grep -r 'status: DRIFT'   tmp/ 2>/dev/null | wc -l", returnStdout: true).trim()
    def missing = sh(script: "grep -r 'status: MISSING' tmp/ 2>/dev/null | wc -l", returnStdout: true).trim()
    return ":white_check_mark: DB Config Sync #${BUILD_NUMBER}\n" +
           "Drifts fixed: ${drifts} | Missing objects: ${missing}\n" +
           "Report: ${BUILD_URL}artifact/reports/"
}
