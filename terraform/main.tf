provider "google" {
  project = var.project
}

locals {
  nodes = {
    central-pl = {
      zone = "europe-central2-a"
      role = "central"
    }
    agent-us = {
      zone = "us-central1-a"
      role = "agent"
    }
    agent-pl = {
      zone = "europe-central2-a"
      role = "agent"
    }
    agent-jp = {
      zone = "asia-northeast1-a"
      role = "agent"
    }
    agent-au = {
      zone = "australia-southeast1-a"
      role = "agent"
    }
  }
}

data "google_compute_image" "ubuntu" {
  family  = "ubuntu-2204-lts"
  project = "ubuntu-os-cloud"
}

resource "google_compute_network" "stun_monitor" {
  name                    = "stun-monitor-network"
  auto_create_subnetworks = true
}

resource "google_compute_firewall" "ssh" {
  name    = "stun-monitor-ssh"
  network = google_compute_network.stun_monitor.name

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = var.ssh_source_ranges
  target_tags   = ["stun-monitor"]
}

resource "google_compute_firewall" "kafka_internal" {
  name    = "stun-monitor-kafka-internal"
  network = google_compute_network.stun_monitor.name

  allow {
    protocol = "tcp"
    ports    = ["9092"]
  }

  source_tags = ["stun-monitor"]
  target_tags = ["stun-monitor"]
}

resource "google_compute_instance" "node" {
  for_each     = local.nodes
  name         = each.key
  machine_type = var.machine_type
  zone         = each.value.zone
  tags         = ["stun-monitor", each.value.role]

  boot_disk {
    initialize_params {
      image = data.google_compute_image.ubuntu.self_link
      size  = 20
      type  = "pd-balanced"
    }
  }

  network_interface {
    network = google_compute_network.stun_monitor.name

    access_config {
    }
  }

  metadata = {
    ssh-keys = "${var.ssh_user}:${file(pathexpand(var.ssh_public_key_path))}"
    role     = each.value.role
  }
}
