# Copyright © 2026 Anterior <tech@anterior.com>
# SPDX-License-Identifier: AGPL-3.0-only

{
  pkgs,
  self,
  elasticmq,
  ...
}:
let
  awsConfig = {
    AWS_DEFAULT_REGION = "us-east-1";
    AWS_REGION = "us-east-1";
    AWS_ENDPOINT_URL_SQS = "http://datastores:9324";
    AWS_ACCESS_KEY_ID = "fake";
    AWS_SECRET_ACCESS_KEY = "fake";
  };
in
pkgs.testers.runNixOSTest {
  name = "test-watchdawg";
  globalTimeout = 5 * 60;
  nodes = {
    datastores =
      { config, pkgs, ... }:
      {
        imports = [ elasticmq ];
        systemd.enableStrictShellChecks = true;
        services.elasticmq = {
          enable = true;
          openFirewall = true;
        };
        systemd.services.elasticmq = {
          postStart = "wait-for-port ${toString config.services.elasticmq.port}";
          path = [ pkgs.wait-for-port ];
        };
        systemd.services.elasticmq-init = {
          before = [ "multi-user.target" ];
          requiredBy = [ "multi-user.target" ];
          after = [ "elasticmq.service" ];
          requires = [ "elasticmq.service" ];
          serviceConfig = {
            Type = "oneshot";
            Restart = "no";
          };
          environment = awsConfig;
          script = ''
            aws sqs create-queue --queue-name jobQueue
            aws sqs create-queue --queue-name jobQueueDl
            aws sqs create-queue --queue-name watchdogQueue
            aws sqs set-queue-attributes \
              --queue-url http://localhost:4566/000000000000/jobQueue \
              --attributes '{ "RedrivePolicy": "{\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-1:000000000000:jobQueueDl\",\"maxReceiveCount\":\"3\"}" }'
          '';
          path = [ pkgs.awscli2 ];
        };
      };
    watchdawg =
      { pkgs, ... }:
      {
        systemd.services.watchdawg =
          let
            inherit (pkgs.stdenv.hostPlatform) system;
          in
          {
            serviceConfig = {
              Type = "oneshot";
              Restart = "no";
              RemainAfterExit = "yes";
              ExecStart = "${self.packages.${system}.watchdawg}/bin/demo-main";
            };
            preStart = "";
            environment = {
              orchestratorDomain = "http://orchestrator:9991";
              jobUrl = "http://datastores:9324/000000000000/jobQueue";
              watchdogUrl = "http://datastores:9324/000000000000/watchdogQueue";
              jobVisibilityTimeoutSecs = "10";
              watchdogIntervalSecs = "2";
              maxWatchdogAgeSecs = "5";
            }
            // awsConfig;
            wants = [ "multi-user.target" ];
          };
      };
    orchestrator =
      { pkgs, ... }:
      {
        networking.firewall.allowedTCPPorts = [ 9991 ];
        systemd.services.orchestrator =
          let
            inherit (pkgs.stdenv.hostPlatform) system;
          in
          {
            serviceConfig = {
              Type = "oneshot";
              Restart = "no";
              RemainAfterExit = "yes";
              ExecStart = "${self.packages.${system}.orchestrator}/bin/orchestrator";
            };
            preStart = "";
            environment = { };
            wants = [ "multi-user.target" ];
          };
      };
    tester =
      { pkgs, ... }:
      {
        systemd.services.tester =
          let
            inherit (pkgs.stdenv.hostPlatform) system;
          in
          {
            serviceConfig = {
              Type = "oneshot";
              Restart = "no";
              RemainAfterExit = "yes";
              ExecStart = "${self.packages.${system}.watchdawg}/bin/integration-test";
            };
            preStart = "";
            environment = {
              orchestratorDomain = "http://orchestrator:9991";
              jobUrl = "http://datastores:9324/000000000000/jobQueue";
            }
            // awsConfig;
            wants = [ "multi-user.target" ];
          };
      };
  };
  testScript = ''
    start_all()

    datastores.wait_for_unit("default.target")

    watchdawg.systemctl("start --no-block watchdawg.service")
    orchestrator.systemctl("start --no-block orchestrator.service")

    watchdawg.wait_for_unit("default.target")
    orchestrator.wait_for_unit("default.target")

    tester.succeed("systemctl start tester.service")
  '';
}
