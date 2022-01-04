<?php
declare(strict_types=1);

namespace App\Controller;

/**
 * TipoOperacoesFinanceiras Controller
 *
 * @property \App\Model\Table\TipoOperacoesFinanceirasTable $TipoOperacoesFinanceiras
 * @method \App\Model\Entity\TipoOperacoesFinanceira[]|\Cake\Datasource\ResultSetInterface paginate($object = null, array $settings = [])
 */
class TipoOperacoesFinanceirasController extends AppController
{
    /**
     * Index method
     *
     * @return \Cake\Http\Response|null|void Renders view
     */
    public function index()
    {
        $tipoOperacoesFinanceiras = $this->paginate($this->TipoOperacoesFinanceiras);

        $this->set(compact('tipoOperacoesFinanceiras'));
    }

    /**
     * View method
     *
     * @param string|null $id Tipo Operacoes Financeira id.
     * @return \Cake\Http\Response|null|void Renders view
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function view($id = null)
    {
        $tipoOperacoesFinanceira = $this->TipoOperacoesFinanceiras->get($id, [
            'contain' => ['OperacoesFinanceiras'],
        ]);

        $this->set(compact('tipoOperacoesFinanceira'));
    }

    /**
     * Add method
     *
     * @return \Cake\Http\Response|null|void Redirects on successful add, renders view otherwise.
     */
    public function add()
    {
        $tipoOperacoesFinanceira = $this->TipoOperacoesFinanceiras->newEmptyEntity();
        if ($this->request->is('post')) {
            $tipoOperacoesFinanceira = $this->TipoOperacoesFinanceiras->patchEntity($tipoOperacoesFinanceira, $this->request->getData());
            if ($this->TipoOperacoesFinanceiras->save($tipoOperacoesFinanceira)) {
                $this->Flash->success(__('The tipo operacoes financeira has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The tipo operacoes financeira could not be saved. Please, try again.'));
        }
        $this->set(compact('tipoOperacoesFinanceira'));
    }

    /**
     * Edit method
     *
     * @param string|null $id Tipo Operacoes Financeira id.
     * @return \Cake\Http\Response|null|void Redirects on successful edit, renders view otherwise.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function edit($id = null)
    {
        $tipoOperacoesFinanceira = $this->TipoOperacoesFinanceiras->get($id, [
            'contain' => [],
        ]);
        if ($this->request->is(['patch', 'post', 'put'])) {
            $tipoOperacoesFinanceira = $this->TipoOperacoesFinanceiras->patchEntity($tipoOperacoesFinanceira, $this->request->getData());
            if ($this->TipoOperacoesFinanceiras->save($tipoOperacoesFinanceira)) {
                $this->Flash->success(__('The tipo operacoes financeira has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The tipo operacoes financeira could not be saved. Please, try again.'));
        }
        $this->set(compact('tipoOperacoesFinanceira'));
    }

    /**
     * Delete method
     *
     * @param string|null $id Tipo Operacoes Financeira id.
     * @return \Cake\Http\Response|null|void Redirects to index.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function delete($id = null)
    {
        $this->request->allowMethod(['post', 'delete']);
        $tipoOperacoesFinanceira = $this->TipoOperacoesFinanceiras->get($id);
        if ($this->TipoOperacoesFinanceiras->delete($tipoOperacoesFinanceira)) {
            $this->Flash->success(__('The tipo operacoes financeira has been deleted.'));
        } else {
            $this->Flash->error(__('The tipo operacoes financeira could not be deleted. Please, try again.'));
        }

        return $this->redirect(['action' => 'index']);
    }
}
