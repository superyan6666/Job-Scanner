export function scoreJobs(jobs) {
  if (!jobs.length) return [];

  const maxSalary = Math.max(...jobs.map(j => j.maxSalary || j.salary || 0));

  return jobs.map(job => {
    let score = 0;
    const salary = job.maxSalary || job.salary || 0;

    if (maxSalary > 0) score += (salary / maxSalary) * 40;

    const matchCount = (job.keywords || [])
      .filter(kw => (job.title || '').toLowerCase().includes(kw.toLowerCase()))
      .length;
    score += Math.min(matchCount * 6, 30);

    const welfare = (job.welfare || '').toLowerCase();
    if (welfare.includes('双休') || welfare.includes('周末双休')) score += 10;
    if (welfare.includes('五险一金') || welfare.includes('六险一金')) score += 10;
    if (welfare.includes('弹性工作')) score += 5;
    if (welfare.includes('不加班')) score += 5;

    const experience = (job.experience || '');
    if (experience.includes('1-3年') || experience.includes('3-5年')) score += 10;

    return { ...job, score: Math.round(score * 100) / 100 };
  })
  .sort((a, b) => (b.score - a.score) || (b.salary - a.salary));
}

export function extractExperienceEducation(tags) {
  let experience = '', education = '';
  const expKeywords = ['年', '应届', '经验'];
  const eduKeywords = ['本科', '大专', '硕士', '博士', '中专', '高中', '学历'];

  for (const tag of tags) {
    const clean = tag.trim();
    if (!experience && expKeywords.some(k => clean.includes(k))) experience = clean;
    if (!education && eduKeywords.some(k => clean.includes(k))) education = clean;
  }

  return { experience, education };
}

export function parseSalary(salaryStr) {
  if (!salaryStr) return { min: 0, max: 0, avg: 0 };

  const str = salaryStr.trim();
  let bonusMonth = 12;
  let mainPart = str;

  if (str.includes('·') || str.includes('·')) {
    const sep = str.includes('·') ? '·' : '·';
    const parts = str.split(sep);
    mainPart = parts[0].trim();
    const bonusMatch = parts[1]?.match(/(\d+)\s*薪/);
    if (bonusMatch) bonusMonth = parseInt(bonusMatch[1]);
  }

  const rangeMatch = mainPart.match(/^(\d+\.?\d*)\s*[-~至]\s*(\d+\.?\d*)\s*([Kk万薪元])/);
  if (rangeMatch) {
    const low = parseFloat(rangeMatch[1]);
    const high = parseFloat(rangeMatch[2]);
    const unit = rangeMatch[3];
    const multiplier = unit.toLowerCase() === 'k' ? 1000 : unit === '万' ? 10000 : 1;
    return {
      min: low * multiplier,
      max: high * multiplier,
      avg: Math.round((low + high) / 2 * multiplier * bonusMonth / 12),
    };
  }

  const fixedMatch = mainPart.match(/^(\d+\.?\d*)\s*([Kk万薪元])/);
  if (fixedMatch) {
    const val = parseFloat(fixedMatch[1]);
    const unit = fixedMatch[2];
    const multiplier = unit.toLowerCase() === 'k' ? 1000 : unit === '万' ? 10000 : 1;
    return {
      min: val * multiplier,
      max: val * multiplier,
      avg: Math.round(val * multiplier * bonusMonth / 12),
    };
  }

  return { min: 0, max: 0, avg: 0 };
}