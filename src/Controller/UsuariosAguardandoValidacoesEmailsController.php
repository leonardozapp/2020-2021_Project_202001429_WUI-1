<?php
declare(strict_types=1);

namespace App\Controller;

/**
 * UsuariosAguardandoValidacoesEmails Controller
 *
 * @property \App\Model\Table\UsuariosAguardandoValidacoesEmailsTable $UsuariosAguardandoValidacoesEmails
 * @method \App\Model\Entity\UsuariosAguardandoValidacoesEmail[]|\Cake\Datasource\ResultSetInterface paginate($object = null, array $settings = [])
 */
class UsuariosAguardandoValidacoesEmailsController extends AppController
{
    /**
     * Index method
     *
     * @return \Cake\Http\Response|null|void Renders view
     */
    public function index()
    {
        $usuariosAguardandoValidacoesEmails = $this->paginate($this->UsuariosAguardandoValidacoesEmails);

        $this->set(compact('usuariosAguardandoValidacoesEmails'));
    }

    /**
     * View method
     *
     * @param string|null $id Usuarios Aguardando Validacoes Email id.
     * @return \Cake\Http\Response|null|void Renders view
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function view($id = null)
    {
        $usuariosAguardandoValidacoesEmail = $this->UsuariosAguardandoValidacoesEmails->get($id, [
            'contain' => [],
        ]);

        $this->set(compact('usuariosAguardandoValidacoesEmail'));
    }

    /**
     * Add method
     *
     * @return \Cake\Http\Response|null|void Redirects on successful add, renders view otherwise.
     */
    public function add()
    {
        $usuariosAguardandoValidacoesEmail = $this->UsuariosAguardandoValidacoesEmails->newEmptyEntity();
        if ($this->request->is('post')) {
            $usuariosAguardandoValidacoesEmail = $this->UsuariosAguardandoValidacoesEmails->patchEntity($usuariosAguardandoValidacoesEmail, $this->request->getData());
            if ($this->UsuariosAguardandoValidacoesEmails->save($usuariosAguardandoValidacoesEmail)) {
                $this->Flash->success(__('The usuarios aguardando validacoes email has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The usuarios aguardando validacoes email could not be saved. Please, try again.'));
        }
        $this->set(compact('usuariosAguardandoValidacoesEmail'));
    }

    /**
     * Edit method
     *
     * @param string|null $id Usuarios Aguardando Validacoes Email id.
     * @return \Cake\Http\Response|null|void Redirects on successful edit, renders view otherwise.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function edit($id = null)
    {
        $usuariosAguardandoValidacoesEmail = $this->UsuariosAguardandoValidacoesEmails->get($id, [
            'contain' => [],
        ]);
        if ($this->request->is(['patch', 'post', 'put'])) {
            $usuariosAguardandoValidacoesEmail = $this->UsuariosAguardandoValidacoesEmails->patchEntity($usuariosAguardandoValidacoesEmail, $this->request->getData());
            if ($this->UsuariosAguardandoValidacoesEmails->save($usuariosAguardandoValidacoesEmail)) {
                $this->Flash->success(__('The usuarios aguardando validacoes email has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The usuarios aguardando validacoes email could not be saved. Please, try again.'));
        }
        $this->set(compact('usuariosAguardandoValidacoesEmail'));
    }

    /**
     * Delete method
     *
     * @param string|null $id Usuarios Aguardando Validacoes Email id.
     * @return \Cake\Http\Response|null|void Redirects to index.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function delete($id = null)
    {
        $this->request->allowMethod(['post', 'delete']);
        $usuariosAguardandoValidacoesEmail = $this->UsuariosAguardandoValidacoesEmails->get($id);
        if ($this->UsuariosAguardandoValidacoesEmails->delete($usuariosAguardandoValidacoesEmail)) {
            $this->Flash->success(__('The usuarios aguardando validacoes email has been deleted.'));
        } else {
            $this->Flash->error(__('The usuarios aguardando validacoes email could not be deleted. Please, try again.'));
        }

        return $this->redirect(['action' => 'index']);
    }
}
