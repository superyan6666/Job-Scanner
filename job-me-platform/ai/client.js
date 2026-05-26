import fetch from 'node-fetch';

const AI_CLIENTS = {};

export function createAIClient(config) {
  const { provider, endpoint, apiKey, model, temperature = 0.7 } = config;
  const key = `${provider}:${endpoint}`;

  if (AI_CLIENTS[key]) return AI_CLIENTS[key];

  const client = {
    provider,
    model,
    endpoint,
    apiKey,

    async chat(messages, options = {}) {
      const body = {
        model: options.model || model,
        messages,
        temperature: options.temperature ?? temperature,
        max_tokens: options.maxTokens || 500,
      };

      const headers = {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${apiKey}`,
      };

      if (provider === 'ollama') {
        headers['Authorization'] = 'Bearer ollama';
      }

      const response = await fetch(endpoint, {
        method: 'POST',
        headers,
        body: JSON.stringify(body),
      });

      if (!response.ok) {
        const text = await response.text();
        throw new Error(`AI API 错误 (${response.status}): ${text}`);
      }

      const data = await response.json();
      return data.choices?.[0]?.message?.content || '';
    },

    async generateReply(userMessage, systemPrompt) {
      const messages = [
        { role: 'system', content: systemPrompt },
        { role: 'user', content: userMessage },
      ];
      return this.chat(messages);
    },

    async analyzeJob(jobDescription) {
      const prompt = `你是一个资深的招聘分析师。请分析以下职位描述，给出：
1. 岗位匹配度评分 (0-100)
2. 三个关键匹配理由
3. 两个潜在风险/疑虑点
4. 建议的打招呼策略

职位描述：
${jobDescription}`;

      return this.chat([{ role: 'user', content: prompt }], { temperature: 0.5 });
    },

    async optimizeResume(jobTitle, companyName, originalResume) {
      const prompt = `我正在应聘 ${companyName} 的 ${jobTitle} 职位。
请帮我优化以下自我介绍/文本简历，使其更匹配该岗位，突出相关经验。保持简洁（不超过100字）。

我的原始简历：
${originalResume}`;

      return this.chat([{ role: 'user', content: prompt }], { temperature: 0.6 });
    },
  };

  AI_CLIENTS[key] = client;
  return client;
}

export const DEFAULT_SYSTEM_PROMPT = `你是一个求职者，正在和招聘HR沟通。
请根据你的简历提取亮点，简短礼貌地回复HR的消息，突出匹配度。
回复要简洁(不超过80字)、真诚、专业。
不要过度承诺，不要提薪资要求除非HR主动问。
如果HR提到面试、微信等，表达积极意愿。`;

export function buildSystemPrompt(resumeText) {
  if (!resumeText) return DEFAULT_SYSTEM_PROMPT;
  return `${DEFAULT_SYSTEM_PROMPT}\n\n我的简历：\n${resumeText}`;
}