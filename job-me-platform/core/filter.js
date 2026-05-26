function parseKeywords(str) {
  return (str || '').toLowerCase().split(/[，,]/).map(s => s.trim()).filter(Boolean);
}

function filterJobs(jobs, filters) {
  const {
    includeKeywords = [],
    excludeKeywords = [],
    locationKeywords = [],
    minSalaryK = 0,
    excludeDegrees = [],
    maxUpdateDays = 0,
    excludeHeadhunters = false,
  } = filters;

  return jobs.filter(job => {
    const title = (job.title || '').toLowerCase();
    const company = (job.company || '').toLowerCase();
    const location = (job.location || '').toLowerCase();

    if (excludeHeadhunters && job.isHeadhunter) return false;
    if (includeKeywords.length && !includeKeywords.some(k => title.includes(k))) return false;
    if (excludeKeywords.length && excludeKeywords.some(k => title.includes(k) || company.includes(k))) return false;
    if (locationKeywords.length && !locationKeywords.some(k => location.includes(k))) return false;
    if (minSalaryK && job.maxSalary && job.maxSalary < minSalaryK) return false;

    const btnText = (job.btnText || '').toLowerCase();
    if (['继续沟通', '已申请', '已投递', '已沟通', '已回复'].some(t => btnText.includes(t))) return false;

    return true;
  });
}

function companyBlacklists() {
  return {
    outsourcing: { name: '外包', color: '#f59e0b', keywords: ['软通动力', '中软国际', '博彦科技', '润和软件', '佰钧成', '法本信息', '东软', '京北方', '宇信科技', '文思海辉', '诚迈科技'] },
    training: { name: '培训机构', color: '#3b82f6', keywords: ['达内', '传智播客', '黑马程序员', '尚硅谷', '千锋'] },
  };
}

module.exports = { parseKeywords, filterJobs, companyBlacklists };