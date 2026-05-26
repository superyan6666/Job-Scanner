export function processTemplate(text, variables = {}) {
  if (!text) return '';
  const { hrName = '您', jobName = '该职位', companyName = '贵司' } = variables;
  return text
    .replace(/\{HR\}/gi, hrName)
    .replace(/\{职位\}/gi, jobName)
    .replace(/\{公司\}/gi, companyName);
}

export function parseQuickReplies(str) {
  const lines = (str || '').split('\n').map(l => l.trim()).filter(Boolean);
  const result = [];

  for (const line of lines) {
    let catName = '默认';
    let repliesStr = line;

    const colonIndex = line.search(/[:：]/);
    if (colonIndex > 0) {
      catName = line.slice(0, colonIndex);
      repliesStr = line.slice(colonIndex + 1);
    }

    const replies = repliesStr.split(/[，,]/).map(s => s.trim()).filter(Boolean);
    result.push({ name: catName, items: replies });
  }

  if (!result.length) result.push({ name: '通用', items: [] });
  return result;
}

export function generateGreeting(hrName, jobName, companyName, customGreeting) {
  if (customGreeting) {
    return processTemplate(customGreeting, { hrName, jobName, companyName });
  }
  return `${hrName}您好，我对${jobName || '贵司的在招职位'}很感兴趣，方便了解一下详细情况吗？`;
}

export function generateTextResume(resumeText, hrName, jobName, companyName) {
  return processTemplate(resumeText || '', { hrName, jobName, companyName });
}