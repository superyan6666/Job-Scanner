export function parseKeywords(str) {
  return (str || '').toLowerCase().split(/[，,]/).map(s => s.trim()).filter(Boolean);
}

export function filterJobs(jobs, filters) {
  const {
    includeKeywords = [],
    excludeKeywords = [],
    locationKeywords = [],
    minSalaryK = 0,
    excludeDegrees = [],
    maxUpdateDays = 0,
  } = filters;

  return jobs.filter(job => {
    const title = (job.title || '').toLowerCase();
    const company = (job.company || '').toLowerCase();
    const location = (job.location || '').toLowerCase();
    const cardText = (job.cardText || title + ' ' + company).toLowerCase();

    if (filters.excludeHeadhunters && job.isHeadhunter) return false;

    if (includeKeywords.length && !includeKeywords.some(k => title.includes(k))) return false;
    if (excludeKeywords.length && excludeKeywords.some(k => title.includes(k) || company.includes(k))) return false;
    if (locationKeywords.length && !locationKeywords.some(k => location.includes(k))) return false;

    if (minSalaryK && job.maxSalary && job.maxSalary < minSalaryK) return false;

    if (excludeDegrees.length && job.education) {
      if (excludeDegrees.some(deg => job.education.includes(deg))) return false;
    }

    if (maxUpdateDays && job.updateTime) {
      const days = (Date.now() - job.updateTime) / (1000 * 60 * 60 * 24);
      if (days > maxUpdateDays) return false;
    }

    const btnText = (job.btnText || '').toLowerCase();
    if (['继续沟通', '已申请', '已投递', '已沟通', '已回复'].some(t => btnText.includes(t))) return false;

    return true;
  });
}

export function companyBlacklists() {
  return {
    scam: {
      name: '风险预警', color: '#ef4444',
      keywords: ['华安', '虚假投资', '刷单', '兼职诈骗', '理财诈骗'],
    },
    outsourcing: {
      name: '外包', color: '#f59e0b',
      keywords: [
        '软通动力', '中软国际', '博彦科技', '易思博', '润和软件',
        '佰钧成', '睿服科技', '亿达信', '微创软件', '德科信息',
        '外企德科', '法本信息', '东软', '京北方', '宇信科技',
        '文思海辉', '海隆软件', '信华信', '易宝软件', '拓保软件', '诚迈科技',
      ],
    },
    training: {
      name: '培训机构', color: '#3b82f6',
      keywords: ['达内', '传智播客', '黑马程序员', '尚硅谷', '千锋', '动力节点', '马士兵', '咕泡', '拉勾教育', '开课吧', '中公教育', '猿辅导'],
    },
  };
}

export function checkCompanyTags(companyName) {
  const tags = [];
  const lists = companyBlacklists();
  for (const [, bl] of Object.entries(lists)) {
    if (bl.keywords.some(kw => companyName.includes(kw))) {
      tags.push({ name: bl.name, color: bl.color, emoji: bl.emoji || '⚠️' });
    }
  }
  return tags;
}