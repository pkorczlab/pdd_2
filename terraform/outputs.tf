output "nodes" {
  value = {
    for name, instance in google_compute_instance.node : name => {
      role        = local.nodes[name].role
      zone        = local.nodes[name].zone
      public_ip   = instance.network_interface[0].access_config[0].nat_ip
      internal_ip = instance.network_interface[0].network_ip
    }
  }
}
