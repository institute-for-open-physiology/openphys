import SwiftUI
import H10Kit

struct SyncView: View {
    @ObservedObject var model: AppModel
    @State private var selected: Remote?
    @State private var adding = false
    @Environment(\.accessibilityReduceMotion) private var reduceMotion
    @Environment(\.scenePhase) private var phase

    var body: some View {
        Page(spacing: 28) {
            PageTitle(title: "Sync")
            if let error = model.remoteError { Text(error).font(.footnote).foregroundStyle(Palette.bad) }
            ZStack(alignment: .topLeading) {
                TimelineView(.animation(minimumInterval: 1.0 / 30, paused: reduceMotion || phase != .active)) { tick in
                    Canvas { context, _ in
                        for index in 0...model.remotes.count {
                            let endY = 89.0 + Double(index) * 124 + 54
                            let path = Path { p in
                                p.move(to: CGPoint(x: 21, y: 60)); p.addLine(to: CGPoint(x: 21, y: endY - 14))
                                p.addQuadCurve(to: CGPoint(x: 35, y: endY), control: CGPoint(x: 21, y: endY)); p.addLine(to: CGPoint(x: 48, y: endY))
                            }
                            context.stroke(path, with: .color(Palette.line), lineWidth: 1)
                            if !reduceMotion, index < model.remotes.count, model.isTransferring(model.remotes[index], at: tick.date) {
                                let phase = (tick.date.timeIntervalSinceReferenceDate / 3.5 + Double(index) * 0.18).truncatingRemainder(dividingBy: 1)
                                for offset in [0.0, 0.5] {
                                    let fraction = (phase + offset).truncatingRemainder(dividingBy: 1)
                                    if let position = path.trimmedPath(from: 0, to: max(0.001, fraction)).currentPoint {
                                        context.fill(Path(ellipseIn: CGRect(x: position.x - 2, y: position.y - 2, width: 4, height: 4)), with: .color(Palette.up.opacity(0.8)))
                                    }
                                }
                            }
                        }
                    }.accessibilityHidden(true)
                }
                VStack(alignment: .leading, spacing: 37) {
                    HStack(spacing: 20) {
                        Image(systemName: "iphone").font(.system(size: 27, weight: .light)).foregroundStyle(Palette.muted).frame(width: 26)
                        VStack(alignment: .leading, spacing: 6) {
                            Text("This iPhone").font(.system(size: 16, weight: .medium))
                            Text("Your recordings").font(.system(size: 12)).foregroundStyle(Palette.muted)
                        }
                    }.padding(.horizontal, 8).frame(height: 52)
                    VStack(spacing: 16) {
                        ForEach(model.remotes) { remote in
                            Button { selected = remote } label: {
                                HStack(spacing: 16) {
                                    Image(systemName: "server.rack").font(.system(size: 23, weight: .light)).foregroundStyle(Palette.muted).frame(width: 26)
                                    VStack(alignment: .leading, spacing: 8) {
                                        Text(remote.name).font(.system(size: 16, weight: .semibold))
                                        TimelineView(.periodic(from: .now, by: 1)) { _ in
                                            HStack(spacing: 6) {
                                                StatusDot(level: model.summary(remote).level)
                                                Text(model.summary(remote).word).font(.system(size: 12)).foregroundStyle(Palette.muted)
                                            }
                                        }
                                    }
                                    Spacer(minLength: 4)
                                    Image(systemName: "chevron.right").font(.system(size: 11)).foregroundStyle(Palette.faint)
                                }.frame(height: 108).contentShape(Rectangle())
                            }.buttonStyle(.plain).accessibilityIdentifier("remote-" + remote.id)
                        }
                        HealthExportRow(health: model.health)
                    }.padding(.leading, 58)
                }
            }.padding(.top, 8)
            Button { adding = true } label: { Label("Add server", systemImage: "plus").font(.system(size: 14, weight: .semibold)).frame(maxWidth: .infinity, minHeight: 44) }.buttonStyle(.plain).disabled(model.remoteError != nil)
        }
        .sheet(item: $selected) { remote in RemoteDetail(model: model, id: remote.id) }
        .sheet(isPresented: $adding) { RemoteLogin(model: model) }
    }
}

private struct TransferFlow: View {
    let active: Bool
    @Environment(\.accessibilityReduceMotion) private var reduceMotion
    var body: some View {
        HStack(spacing: 22) {
            endpoint("iphone", "This iPhone")
            TimelineView(.animation(minimumInterval: 1.0 / 30, paused: !active || reduceMotion)) { tick in
                Canvas { context, size in
                    let y = size.height / 2
                    context.stroke(Path { $0.move(to: CGPoint(x: 0, y: y)); $0.addLine(to: CGPoint(x: size.width, y: y)) }, with: .color(Palette.line), lineWidth: 1)
                    context.stroke(Path { $0.move(to: CGPoint(x: size.width - 4, y: y - 3)); $0.addLine(to: CGPoint(x: size.width, y: y)); $0.addLine(to: CGPoint(x: size.width - 4, y: y + 3)) }, with: .color(Palette.faint), lineWidth: 1)
                    if active && !reduceMotion {
                        for offset in [0.0, 0.5] {
                            let fraction = (tick.date.timeIntervalSinceReferenceDate / 2.8 + offset).truncatingRemainder(dividingBy: 1)
                            context.fill(Path(ellipseIn: CGRect(x: fraction * size.width - 2, y: y - 2, width: 4, height: 4)), with: .color(Palette.up.opacity(0.8)))
                        }
                    }
                }
            }.frame(height: 34).padding(.bottom, 28)
            endpoint("server.rack", "Server")
        }.accessibilityHidden(true)
    }
    private func endpoint(_ symbol: String, _ text: String) -> some View {
        VStack(spacing: 13) {
            Image(systemName: symbol).font(.system(size: 30, weight: .light)).frame(height: 34)
            Text(text).font(.system(size: 12)).foregroundStyle(Palette.muted)
        }.fixedSize()
    }
}

private struct RemoteDetail: View {
    @ObservedObject var model: AppModel
    let id: String
    @Environment(\.dismiss) private var dismiss
    @State private var login = false
    @State private var removing = false
    @State private var signingOut = false
    private var remote: Remote? { model.remotes.first { $0.id == id } }

    var body: some View {
        NavigationStack {
            if let remote {
                ScrollView {
                    VStack(alignment: .leading, spacing: 24) {
                        TimelineView(.periodic(from: .now, by: 1)) { tick in
                            VStack(spacing: 30) {
                                TransferFlow(active: model.isTransferring(remote, at: tick.date)).padding(.horizontal, 12)
                                HStack {
                                    VStack(alignment: .leading, spacing: 8) {
                                        HStack(spacing: 8) { StatusDot(level: model.summary(remote).level); Text(model.summary(remote).word).font(.system(size: 16, weight: .medium)) }
                                        Text(receipt(remote, at: tick.date)).font(.system(size: 12)).foregroundStyle(Palette.muted).padding(.leading, 14)
                                    }
                                    Spacer(minLength: 8)
                                    if model.signedIn.contains(id) {
                                        Button { model.setSending(id, !remote.sending) } label: {
                                            Label(remote.sending ? "Pause sync" : "Resume", systemImage: remote.sending ? "pause" : "play")
                                                .font(.system(size: 13, weight: .semibold)).frame(minHeight: 44)
                                        }.buttonStyle(.plain).accessibilityIdentifier("pause-sync")
                                    }
                                }
                            }
                        }
                        Divider().overlay(Palette.line)
                        VStack(alignment: .leading, spacing: 9) {
                            Text("Account").font(.system(size: 12)).foregroundStyle(Palette.muted)
                            Text(model.signedIn.contains(id) ? (remote.account ?? "Connected") : "Signed out").font(.system(size: 14)).textSelection(.enabled)
                        }
                        Divider().overlay(Palette.line)
                        if let url = remote.url {
                            Link(destination: url) {
                                VStack(alignment: .leading, spacing: 9) {
                                    Text("Server").font(.system(size: 12)).foregroundStyle(Palette.muted)
                                    HStack { Text(remote.origin.replacingOccurrences(of: "https://", with: "")).font(.system(size: 13)); Spacer(); Image(systemName: "arrow.up.right").font(.system(size: 14)).foregroundStyle(Palette.muted) }
                                }
                            }
                        }
                        if let problem = model.status[id]?.problem { Text(problem).font(.footnote).foregroundStyle(Palette.bad) }
                        if !model.signedIn.contains(id) { Button("Connect account") { login = true }.buttonStyle(ActionButtonStyle()) }
                    }.padding(24)
                }.background(Palette.surface).foregroundStyle(Palette.ink)
                    .navigationTitle(remote.name).navigationBarTitleDisplayMode(.inline)
                    .toolbar {
                        ToolbarItem(placement: .cancellationAction) { Button { dismiss() } label: { Image(systemName: "chevron.down") }.accessibilityLabel("Close") }
                        ToolbarItem(placement: .primaryAction) {
                            Menu {
                                if model.signedIn.contains(id) { Button("Sign out", systemImage: "rectangle.portrait.and.arrow.right") { signingOut = true } }
                                Button("Remove server", systemImage: "trash", role: .destructive) { removing = true }
                            } label: { Image(systemName: "ellipsis") }.accessibilityLabel("Server actions")
                        }
                    }
                    .sheet(isPresented: $login) { RemoteLogin(model: model, remote: remote) }
                    .confirmationDialog("Sign out? Queued recordings stay on this iPhone.", isPresented: $signingOut, titleVisibility: .visible) { Button("Sign out", role: .destructive) { model.signOut(id) } }
                    .confirmationDialog("Remove this server and its \((model.status[id]?.left ?? 0).formatted()) queued records?", isPresented: $removing, titleVisibility: .visible) {
                        Button("Remove server", role: .destructive) { if model.remove(id) { dismiss() } }
                    }
            }
        }.tint(Palette.ink).presentationDetents([.height(560), .large]).presentationDragIndicator(.hidden).presentationBackground(Palette.surface)
    }
    private func receipt(_ remote: Remote, at now: Date) -> String {
        if !model.signedIn.contains(id) { return "Connect your account to resume." }
        if !remote.sending { return "Readings stay on this iPhone." }
        if let acknowledged = model.status[id]?.acknowledgedAt {
            return now.timeIntervalSince(acknowledged) < 10 ? "Received just now" : "Received " + acknowledged.formatted(date: .omitted, time: .shortened)
        }
        return "Waiting for the server."
    }
}

private struct RemoteLogin: View {
    @ObservedObject var model: AppModel
    var remote: Remote? = nil
    @Environment(\.dismiss) private var dismiss
    @State private var name = ""
    @State private var address = ""
    @State private var email = ""
    @State private var password = ""
    @State private var busy = false
    @State private var error: String?

    var body: some View {
        NavigationStack {
            ScrollView {
                VStack(spacing: 24) {
                    if remote == nil {
                        VStack(spacing: 0) {
                            field("Name") { TextField("Server name", text: $name) }
                            Divider().overlay(Palette.line)
                            field("Address") { TextField("https://server.example", text: $address).keyboardType(.URL).textInputAutocapitalization(.never).autocorrectionDisabled() }
                        }.background(Palette.surface, in: RoundedRectangle(cornerRadius: 18))
                    }
                    VStack(spacing: 0) {
                        field("Email") { TextField("Email", text: $email).textContentType(.username).keyboardType(.emailAddress).textInputAutocapitalization(.never).autocorrectionDisabled() }
                        Divider().overlay(Palette.line)
                        field("Password") { SecureField("Password", text: $password).textContentType(.password) }
                    }.background(Palette.surface, in: RoundedRectangle(cornerRadius: 18))
                    if let error { Text(error).font(.footnote).foregroundStyle(Palette.bad) }
                    Button {
                        busy = true; error = nil
                        Task {
                            if let remote { error = await model.signIn(remote.id, email: email.trimmingCharacters(in: .whitespacesAndNewlines), password: password) }
                            else { error = await model.add(name: name, address: address, email: email.trimmingCharacters(in: .whitespacesAndNewlines), password: password) }
                            busy = false
                            if error == nil { password = ""; dismiss() }
                        }
                    } label: { HStack(spacing: 10) { if busy { ProgressView() }; Text(busy ? "Connecting…" : "Connect") } }
                    .buttonStyle(ActionButtonStyle()).disabled(busy || email.isEmpty || password.isEmpty || (remote == nil && address.isEmpty)).accessibilityIdentifier("connect-server")
                }.padding(24)
            }.background(Palette.canvas).foregroundStyle(Palette.ink)
                .navigationTitle(remote?.name ?? "Add server").navigationBarTitleDisplayMode(.inline)
                .toolbar { ToolbarItem(placement: .cancellationAction) { Button("Cancel") { dismiss() }.disabled(busy) } }
                .interactiveDismissDisabled(busy).onAppear { email = remote?.account ?? "" }
        }.tint(Palette.ink)
    }
    private func field<Content: View>(_ label: String, @ViewBuilder content: () -> Content) -> some View {
        VStack(alignment: .leading, spacing: 8) { Text(label).font(.system(size: 12)).foregroundStyle(Palette.muted); content().font(.system(size: 16)).frame(minHeight: 25) }
            .padding(16)
    }
}
