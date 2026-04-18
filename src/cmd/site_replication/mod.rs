use std::collections::BTreeSet;

use crate::cmd::SiteResyncStatus;

pub fn get_missing_site_names(
    configured_sites: &[impl AsRef<str>],
    statuses: &[SiteResyncStatus],
) -> Vec<String> {
    let present_sites: BTreeSet<&str> = statuses
        .iter()
        .map(|status| status.depl_id.as_str())
        .filter(|value| !value.is_empty())
        .collect();

    configured_sites
        .iter()
        .map(|site| site.as_ref())
        .filter(|site| !present_sites.contains(site))
        .map(str::to_string)
        .collect()
}
