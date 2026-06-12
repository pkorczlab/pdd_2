variable "project" {
  description = "Google Cloud project id."
  type        = string
}

variable "machine_type" {
  description = "VM type used for all nodes."
  type        = string
  default     = "e2-medium"
}

variable "ssh_user" {
  description = "User name placed in instance metadata ssh key."
  type        = string
}

variable "ssh_public_key_path" {
  description = "Path to the public ssh key used for GCP metadata."
  type        = string
  default     = "~/.ssh/id_ed25519.pub"
}

variable "ssh_source_ranges" {
  description = "CIDR ranges allowed to connect to SSH."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}
