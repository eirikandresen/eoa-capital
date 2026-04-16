exports.handler = async (event) => {
  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, body: 'Method not allowed' };
  }

  let body;
  try {
    body = JSON.parse(event.body);
  } catch {
    return { statusCode: 400, body: 'Bad request' };
  }

  const correct = process.env.SITE_PASSWORD;
  if (!correct) {
    return { statusCode: 500, body: 'Server misconfigured' };
  }

  if (body.password === correct) {
    const token = Buffer.from(`eoa:${Date.now()}:${correct.slice(0,4)}`).toString('base64');
    return {
      statusCode: 200,
      body: JSON.stringify({ ok: true, token }),
    };
  }

  await new Promise(r => setTimeout(r, 400));
  return {
    statusCode: 401,
    body: JSON.stringify({ ok: false }),
  };
};
