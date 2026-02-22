package com.martinchoi.assetportfolio

import android.os.Bundle
import android.webkit.WebView
import android.webkit.WebViewClient
import androidx.appcompat.app.AppCompatActivity

class MainActivity : AppCompatActivity() {

    private lateinit var webView: WebView

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        
        webView = WebView(this)
        setContentView(webView)

        // Enable JavaScript
        webView.settings.javaScriptEnabled = true
        webView.settings.domStorageEnabled = true
        
        // Ensure links open in the WebView instead of external browser
        webView.webViewClient = object : WebViewClient() {
            override fun shouldOverrideUrlLoading(view: WebView?, url: String?): Boolean {
                view?.loadUrl(url ?: "")
                return true
            }
        }

        // ==========================================
        // TODO: [USER] 여기에 실제 모바일 앱 URL을 입력하세요!
        // (예: Vercel 등으로 배포된 React App 주소)
        // Streamlit App URL을 입력해도 되지만, iframe 문제가 여전하다면
        // React App을 별도로 배포하고 그 주소를 쓰는 것이 가장 확실합니다.
        // ==========================================
        val mobileUrl = "https://our-asset-portfolio.streamlit.app"
        
        webView.loadUrl(mobileUrl)
    }

    override fun onBackPressed() {
        if (webView.canGoBack()) {
            webView.goBack()
        } else {
            super.onBackPressed()
        }
    }
}
