<?php
declare(strict_types=1);

namespace App\Controller;

/**
 * TipoPlanos Controller
 *
 * @property \App\Model\Table\TipoPlanosTable $TipoPlanos
 * @method \App\Model\Entity\TipoPlano[]|\Cake\Datasource\ResultSetInterface paginate($object = null, array $settings = [])
 */
class TipoPlanosController extends AppController
{
    /**
     * Index method
     *
     * @return \Cake\Http\Response|null|void Renders view
     */
    public function index()
    {
        $this->paginate = [
            'contain' => ['Permissaos'],
        ];
        $tipoPlanos = $this->paginate($this->TipoPlanos);

        $this->set(compact('tipoPlanos'));
    }

    /**
     * View method
     *
     * @param string|null $id Tipo Plano id.
     * @return \Cake\Http\Response|null|void Renders view
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function view($id = null)
    {
        $tipoPlano = $this->TipoPlanos->get($id, [
            'contain' => ['Permissaos', 'Usuarios'],
        ]);

        $this->set(compact('tipoPlano'));
    }

    /**
     * Add method
     *
     * @return \Cake\Http\Response|null|void Redirects on successful add, renders view otherwise.
     */
    public function add()
    {
        $tipoPlano = $this->TipoPlanos->newEmptyEntity();
        if ($this->request->is('post')) {
            $tipoPlano = $this->TipoPlanos->patchEntity($tipoPlano, $this->request->getData());
            if ($this->TipoPlanos->save($tipoPlano)) {
                $this->Flash->success(__('The tipo plano has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The tipo plano could not be saved. Please, try again.'));
        }
        $permissaos = $this->TipoPlanos->Permissaos->find('list', ['limit' => 200]);
        $this->set(compact('tipoPlano', 'permissaos'));
    }

    /**
     * Edit method
     *
     * @param string|null $id Tipo Plano id.
     * @return \Cake\Http\Response|null|void Redirects on successful edit, renders view otherwise.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function edit($id = null)
    {
        $tipoPlano = $this->TipoPlanos->get($id, [
            'contain' => [],
        ]);
        if ($this->request->is(['patch', 'post', 'put'])) {
            $tipoPlano = $this->TipoPlanos->patchEntity($tipoPlano, $this->request->getData());
            if ($this->TipoPlanos->save($tipoPlano)) {
                $this->Flash->success(__('The tipo plano has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The tipo plano could not be saved. Please, try again.'));
        }
        $permissaos = $this->TipoPlanos->Permissaos->find('list', ['limit' => 200]);
        $this->set(compact('tipoPlano', 'permissaos'));
    }

    /**
     * Delete method
     *
     * @param string|null $id Tipo Plano id.
     * @return \Cake\Http\Response|null|void Redirects to index.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function delete($id = null)
    {
        $this->request->allowMethod(['post', 'delete']);
        $tipoPlano = $this->TipoPlanos->get($id);
        if ($this->TipoPlanos->delete($tipoPlano)) {
            $this->Flash->success(__('The tipo plano has been deleted.'));
        } else {
            $this->Flash->error(__('The tipo plano could not be deleted. Please, try again.'));
        }

        return $this->redirect(['action' => 'index']);
    }
}
