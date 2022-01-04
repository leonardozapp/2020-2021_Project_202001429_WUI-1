<?php
declare(strict_types=1);

namespace App\Controller;

/**
 * CadastroFundos Controller
 *
 * @property \App\Model\Table\CadastroFundosTable $CadastroFundos
 * @method \App\Model\Entity\CadastroFundo[]|\Cake\Datasource\ResultSetInterface paginate($object = null, array $settings = [])
 */
class CadastroFundosController extends AppController
{
    /**
     * Index method
     *
     * @return \Cake\Http\Response|null|void Renders view
     */
    public function index()
    {
        $this->paginate = [
            'contain' => ['CnpjFundos', 'TipoClasseFundos', 'TipoRentabilidadeFundos', 'GestorFundos', 'AdministradorFundos'],
        ];
        $cadastroFundos = $this->paginate($this->CadastroFundos);

        $this->set(compact('cadastroFundos'));
    }

    /**
     * View method
     *
     * @param string|null $id Cadastro Fundo id.
     * @return \Cake\Http\Response|null|void Renders view
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function view($id = null)
    {
        $cadastroFundo = $this->CadastroFundos->get($id, [
            'contain' => ['CnpjFundos', 'TipoClasseFundos', 'TipoRentabilidadeFundos', 'GestorFundos', 'AdministradorFundos'],
        ]);

        $this->set(compact('cadastroFundo'));
    }

    /**
     * Add method
     *
     * @return \Cake\Http\Response|null|void Redirects on successful add, renders view otherwise.
     */
    public function add()
    {
        $cadastroFundo = $this->CadastroFundos->newEmptyEntity();
        if ($this->request->is('post')) {
            $cadastroFundo = $this->CadastroFundos->patchEntity($cadastroFundo, $this->request->getData());
            if ($this->CadastroFundos->save($cadastroFundo)) {
                $this->Flash->success(__('The cadastro fundo has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The cadastro fundo could not be saved. Please, try again.'));
        }
        $cnpjFundos = $this->CadastroFundos->CnpjFundos->find('list', ['limit' => 200]);
        $tipoClasseFundos = $this->CadastroFundos->TipoClasseFundos->find('list', ['limit' => 200]);
        $tipoRentabilidadeFundos = $this->CadastroFundos->TipoRentabilidadeFundos->find('list', ['limit' => 200]);
        $gestorFundos = $this->CadastroFundos->GestorFundos->find('list', ['limit' => 200]);
        $administradorFundos = $this->CadastroFundos->AdministradorFundos->find('list', ['limit' => 200]);
        $this->set(compact('cadastroFundo', 'cnpjFundos', 'tipoClasseFundos', 'tipoRentabilidadeFundos', 'gestorFundos', 'administradorFundos'));
    }

    /**
     * Edit method
     *
     * @param string|null $id Cadastro Fundo id.
     * @return \Cake\Http\Response|null|void Redirects on successful edit, renders view otherwise.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function edit($id = null)
    {
        $cadastroFundo = $this->CadastroFundos->get($id, [
            'contain' => [],
        ]);
        if ($this->request->is(['patch', 'post', 'put'])) {
            $cadastroFundo = $this->CadastroFundos->patchEntity($cadastroFundo, $this->request->getData());
            if ($this->CadastroFundos->save($cadastroFundo)) {
                $this->Flash->success(__('The cadastro fundo has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The cadastro fundo could not be saved. Please, try again.'));
        }
        $cnpjFundos = $this->CadastroFundos->CnpjFundos->find('list', ['limit' => 200]);
        $tipoClasseFundos = $this->CadastroFundos->TipoClasseFundos->find('list', ['limit' => 200]);
        $tipoRentabilidadeFundos = $this->CadastroFundos->TipoRentabilidadeFundos->find('list', ['limit' => 200]);
        $gestorFundos = $this->CadastroFundos->GestorFundos->find('list', ['limit' => 200]);
        $administradorFundos = $this->CadastroFundos->AdministradorFundos->find('list', ['limit' => 200]);
        $this->set(compact('cadastroFundo', 'cnpjFundos', 'tipoClasseFundos', 'tipoRentabilidadeFundos', 'gestorFundos', 'administradorFundos'));
    }

    /**
     * Delete method
     *
     * @param string|null $id Cadastro Fundo id.
     * @return \Cake\Http\Response|null|void Redirects to index.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function delete($id = null)
    {
        $this->request->allowMethod(['post', 'delete']);
        $cadastroFundo = $this->CadastroFundos->get($id);
        if ($this->CadastroFundos->delete($cadastroFundo)) {
            $this->Flash->success(__('The cadastro fundo has been deleted.'));
        } else {
            $this->Flash->error(__('The cadastro fundo could not be deleted. Please, try again.'));
        }

        return $this->redirect(['action' => 'index']);
    }
}
