use serde::{Deserialize, Serialize};
use serde_json::json;

// ...existing code...
#[derive(Clone)]
pub struct ApiClient {
    base_url: String,
    client: reqwest::Client,
}

impl ApiClient {
    pub fn new(base_url: &str) -> Self {
        ApiClient {
            base_url: base_url.to_string(),
            client: reqwest::Client::new(),
        }
    }
// ...existing code...

    pub async fn get_metrics(&self) -> Result<serde_json::Value, String> {
        let res = self.client
            .get(format!("{}/metrics", self.base_url))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        
        res.json().await.map_err(|e| e.to_string())
    }

    pub async fn get_audit_logs(&self) -> Result<Vec<serde_json::Value>, String> {
        let res = self.client
            .get(format!("{}/logs", self.base_url))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        
        res.json().await.map_err(|e| e.to_string())
    }

    pub async fn start_phase(&self, phase: u32, ticks: u32) -> Result<(), String> {
        self.client
            .post(format!("{}/phase/{}/start", self.base_url, phase))
            .json(&json!({ "ticks": ticks }))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    pub async fn stop_phase(&self) -> Result<(), String> {
        self.client
            .post(format!("{}/phase/stop", self.base_url))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    pub async fn export_metrics(&self, format: &str) -> Result<String, String> {
        let res = self.client
            .get(format!("{}/export?format={}", self.base_url, format))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        
        res.text().await.map_err(|e| e.to_string())
    }

    pub async fn chat(&self, message: &str) -> Result<String, reqwest::Error> {
        let request = ChatRequest {
            message: message.to_string(),
        };

        let res = self.client
            .post(format!("{}/chat", self.base_url))
            .json(&request)
            .send()
            .await?;

        let chat_response: ChatResponse = res.json().await?;
        Ok(chat_response.reply)
    }
}

impl Default for ApiClient {
    fn default() -> Self {
        Self::new("http://localhost:5000")
    }
}

#[derive(Serialize, Deserialize)]
struct ChatRequest {
    message: String,
}

#[derive(Serialize, Deserialize)]
struct ChatResponse {
    reply: String,
}
